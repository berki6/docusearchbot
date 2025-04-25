import os
import sys
import time
import signal
import logging
import asyncio
import requests
import arxiv
import urllib3
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pytz import utc
from langdetect import detect
from telegram.error import TelegramError

import telegram
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    JobQueue,
)

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOTAPI")
if not BOT_TOKEN:
    raise ValueError("BOTAPI environment variable not set")

# Rate limiting configuration
RATE_LIMIT_REQUESTS = 5  # Max requests per minute
RATE_LIMIT_WINDOW = 60  # Seconds
user_request_counts: Dict[int, List[float]] = defaultdict(list)

# Localization dictionaries
LOCALES = {
    "en": {
        "welcome": "📚 Welcome to Research Paper Bot! Choose an option:",
        "search_prompt": "Please enter your search keywords (e.g., deep learning):",
        "searching": "🔍 Searching for papers... Please wait.",
        "no_papers": "No papers found. Please try a different search term.",
        "error": "Sorry, an error occurred. Please try again.",
        "rate_limit": "Too many requests. Please wait a minute and try again.",
        "help": "Use the 🔍 Search button or type a topic directly to find academic papers from arXiv.",
        "results_found": "📚 Found {count} papers matching your search.",
        "no_more_papers": "No more papers available for this search.",
        "timeout_message": '⏱️ It\'s been a while since you checked the "Load More" results. You can either click the "Load More" button to continue viewing results, or start a new search using the 🔍 Search button.',
        "session_expired": "Your search session has expired. Please start a new search.",
    },
    "es": {
        "welcome": "📚 ¡Bienvenido al Bot de Artículos de Investigación! Elige una opción:",
        "search_prompt": "Por favor, ingresa tus palabras clave de búsqueda (por ejemplo, aprendizaje profundo):",
        "searching": "🔍 Buscando artículos... Por favor espera.",
        "no_papers": "No se encontraron artículos. Prueba con un término diferente.",
        "error": "Lo siento, ocurrió un error. Por favor intenta de nuevo.",
        "rate_limit": "Demasiadas solicitudes. Por favor espera un minuto e intenta de nuevo.",
        "help": "Usa el botón 🔍 Buscar o escribe un tema directamente para encontrar artículos académicos de arXiv.",
        "results_found": "📚 Encontrados {count} artículos que coinciden con tu búsqueda.",
        "no_more_papers": "No hay más artículos disponibles para esta búsqueda.",
        "timeout_message": '⏱️ Ha pasado un tiempo desde que revisaste los resultados de "Cargar Más". Puedes hacer clic en el botón "Cargar Más" para continuar viendo resultados, o iniciar una nueva búsqueda usando el botón 🔍 Buscar.',
        "session_expired": "Tu sesión de búsqueda ha expirado. Por favor inicia una nueva búsqueda.",
    },
}


# SQLite Database Setup
def init_db():
    conn = sqlite3.connect("user_states.db")
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS user_states
                 (user_id INTEGER PRIMARY KEY,
                  state TEXT,
                  query TEXT,
                  current_page INTEGER,
                  load_more_timestamp TEXT,
                  load_more_message_id INTEGER,
                  last_search_time TEXT)"""
    )
    conn.commit()
    conn.close()


# Initialize database
init_db()


# Input sanitization
def sanitize_input(text: str) -> str:
    """Sanitize user input to prevent injection attacks"""
    text = re.sub(r"[<>;{}]", "", text.strip())
    return text[:500]


# Rate limiting check
def check_rate_limit(user_id: int) -> bool:
    """Check if user has exceeded rate limit"""
    current_time = time.time()
    user_request_counts[user_id] = [
        t for t in user_request_counts[user_id] if current_time - t < RATE_LIMIT_WINDOW
    ]
    if len(user_request_counts[user_id]) >= RATE_LIMIT_REQUESTS:
        return False
    user_request_counts[user_id].append(current_time)
    return True


# User state management
class UserState:
    def __init__(self, user_id):
        self.user_id = user_id
        self.results_per_page = 5
        self.timeout_job = None
        self._load_from_db()

    def _load_from_db(self):
        conn = sqlite3.connect("user_states.db")
        c = conn.cursor()
        c.execute("SELECT * FROM user_states WHERE user_id = ?", (self.user_id,))
        data = c.fetchone()
        conn.close()

        if data:
            self.state = data[1]
            self.query = data[2]
            self.current_page = data[3]
            self.load_more_timestamp = (
                datetime.fromisoformat(data[4]) if data[4] else None
            )
            self.load_more_message_id = data[5]
            self.last_search_time = datetime.fromisoformat(data[6]) if data[6] else None
        else:
            self.state = None
            self.query = None
            self.current_page = 0
            self.load_more_timestamp = None
            self.load_more_message_id = None
            self.last_search_time = None

    def save_to_db(self):
        conn = sqlite3.connect("user_states.db")
        c = conn.cursor()
        c.execute(
            """INSERT OR REPLACE INTO user_states 
                    (user_id, state, query, current_page, load_more_timestamp, 
                     load_more_message_id, last_search_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                self.user_id,
                self.state,
                self.query,
                self.current_page,
                (
                    self.load_more_timestamp.isoformat()
                    if self.load_more_timestamp
                    else None
                ),
                self.load_more_message_id,
                self.last_search_time.isoformat() if self.last_search_time else None,
            ),
        )
        conn.commit()
        conn.close()


user_states: Dict[int, UserState] = {}

# Timeout settings
LOAD_MORE_TIMEOUT = 300  # 5 minutes in seconds


def search_arxiv(query: str, max_results=5):
    try:
        logger.info(f"Searching arXiv using arxiv package for: {query}")

        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            connect=4,
            read=4,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=5, pool_maxsize=5
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.timeout = (15, 90)

        logger.info("Configured session with connect timeout=15s, read timeout=90s")

        search = arxiv.Search(
            query=query, max_results=max_results, sort_by=arxiv.SortCriterion.Relevance
        )

        client = arxiv.Client(page_size=10, delay_seconds=5, num_retries=5)
        client._session = session

        logger.info("Configured arxiv client with page_size=10, delay=5s, retries=5")

        entries = []
        for result in client.results(search):
            try:
                authors = ", ".join(author.name for author in result.authors)[:100]
                published = result.published.strftime("%Y-%m-%d")
                categories = ", ".join(result.categories)[:100]
                title = result.title.strip()
                link = result.entry_id
                summary = (
                    (result.summary.strip()[:500] + "...")
                    if result.summary
                    else "No summary available"
                )

                entries.append(
                    {
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "authors": authors,
                        "published": published,
                        "categories": categories,
                    }
                )
            except Exception as e:
                logger.error(f"Error processing entry: {e}")
                continue

        return entries

    except arxiv.HTTPError as e:
        logger.error(f"HTTP error when accessing arXiv API: {e}")
        return {
            "error": "http",
            "message": "Received HTTP error from arXiv. The service might be temporarily unavailable.",
        }
    except arxiv.UnexpectedEmptyPageError as e:
        logger.error(f"Empty page error from arXiv API: {e}")
        return {
            "error": "empty_page",
            "message": "Received unexpected empty results from arXiv. Please try a different search query.",
        }
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error when accessing arXiv API: {e}")
        return {
            "error": "timeout",
            "message": "The request to arXiv timed out. Please try again later.",
        }
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error when accessing arXiv API: {e}")
        return {
            "error": "connection",
            "message": "Could not connect to arXiv. Please check your internet connection and try again.",
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception when accessing arXiv API: {e}")
        return {
            "error": "request",
            "message": "An error occurred while communicating with arXiv. Please try again later.",
        }
    except Exception as e:
        logger.exception(f"Error in search_arxiv: {e}")
        return {
            "error": "unknown",
            "message": "An unexpected error occurred. Please try again later.",
        }


def get_main_keyboard():
    keyboard = [[KeyboardButton(text="🔍 Search")], [KeyboardButton(text="📖 Help")]]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_states:
        user_states[user_id] = UserState(user_id)

    try:
        lang = detect(update.message.text)[:2] if update.message.text else "en"
        if lang not in LOCALES:
            lang = "en"
    except:
        lang = "en"

    reply_markup = get_main_keyboard()
    await update.message.reply_text(LOCALES[lang]["welcome"], reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_states:
        user_states[user_id] = UserState(user_id)

    try:
        lang = detect(update.message.text)[:2] if update.message.text else "en"
        if lang not in LOCALES:
            lang = "en"
    except:
        lang = "en"

    await update.message.reply_text(
        LOCALES[lang]["help"],
        reply_markup=get_main_keyboard(),
    )


async def handle_message_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    user_id = update.message.from_user.id

    try:
        lang = detect(message_text)[:2] if message_text else "en"
        if lang not in LOCALES:
            lang = "en"
    except:
        lang = "en"

    if message_text == "🔍 Search":
        if user_id not in user_states:
            user_states[user_id] = UserState(user_id)
        user_states[user_id].state = "awaiting_query"
        user_states[user_id].save_to_db()
        await update.message.reply_text(
            LOCALES[lang]["search_prompt"],
            reply_markup=ReplyKeyboardRemove(),
        )
    elif message_text == "📖 Help":
        await update.message.reply_text(
            LOCALES[lang]["help"],
            reply_markup=get_main_keyboard(),
        )


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    try:
        lang = "en"  # Default to English for callback queries
    except:
        lang = "en"

    if data == "action_search":
        if user_id not in user_states:
            user_states[user_id] = UserState(user_id)
        user_states[user_id].state = "awaiting_query"
        user_states[user_id].save_to_db()
        await query.message.reply_text(
            LOCALES[lang]["search_prompt"],
            reply_markup=ReplyKeyboardRemove(),
        )
    elif data == "action_help":
        await query.message.reply_text(
            LOCALES[lang]["help"],
            reply_markup=get_main_keyboard(),
        )


async def cleanup_load_more_state(user_id, context):
    try:
        if user_id in user_states:
            user_state = user_states[user_id]
            if user_state.timeout_job:
                try:
                    user_state.timeout_job.schedule_removal()
                except Exception as e:
                    logger.warning(f"Error removing scheduled job: {e}")
                user_state.timeout_job = None
            user_state.load_more_timestamp = None
            user_state.load_more_message_id = None
            user_state.save_to_db()
            logger.debug(f"Cleaned up Load More state for user {user_id}")
    except Exception as e:
        logger.error(f"Error during cleanup of Load More state: {e}")
        user_states[user_id].load_more_message_id = None
        user_states[user_id].save_to_db()


async def send_load_more_timeout_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    user_id = job.data.get("user_id")
    chat_id = job.data.get("chat_id")

    if user_id in user_states:
        user_state = user_states[user_id]
        if (
            user_state.load_more_timestamp
            and (datetime.now() - user_state.load_more_timestamp).total_seconds()
            >= LOAD_MORE_TIMEOUT
        ):
            try:
                if chat_id:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=LOCALES["en"][
                            "timeout_message"
                        ],  # Default to English for timeout messages
                        reply_markup=get_main_keyboard(),
                    )
                    user_state.timeout_job = None
                    user_state.load_more_timestamp = None
                    user_state.load_more_message_id = None
                    user_state.save_to_db()
            except Exception as e:
                logger.error(f"Error sending timeout message: {e}")


async def handle_load_more(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    try:
        lang = "en"
    except:
        lang = "en"

    logger.info(f"Load More clicked by user {user_id} on message {message_id}")

    if user_id not in user_states or not user_states[user_id].query:
        logger.warning(
            f"Invalid Load More: user_id={user_id}, query={user_states.get(user_id, 'None').query}"
        )
        await query.message.reply_text(
            LOCALES[lang]["session_expired"], reply_markup=get_main_keyboard()
        )
        return

    user_state = user_states[user_id]
    stored_query = user_state.query
    user_state.current_page += 1
    user_state.save_to_db()

    if user_state.timeout_job:
        user_state.timeout_job.schedule_removal()
        user_state.timeout_job = None

    processing_message = await query.message.reply_text(LOCALES[lang]["searching"])

    await send_paper_results(
        update, context, stored_query, processing_message, is_load_more=True, lang=lang
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        message_text = sanitize_input(update.message.text)

        try:
            lang = detect(message_text)[:2]
            if lang not in LOCALES:
                lang = "en"
        except:
            lang = "en"

        if not check_rate_limit(user_id):
            await update.message.reply_text(
                LOCALES[lang]["rate_limit"], reply_markup=get_main_keyboard()
            )
            return

        if message_text in ["🔍 Search", "📖 Help"]:
            return await handle_message_buttons(update, context)

        if user_id not in user_states:
            user_states[user_id] = UserState(user_id)

        if user_states[user_id].timeout_job:
            user_states[user_id].timeout_job.schedule_removal()
            user_states[user_id].timeout_job = None

        if user_states[user_id].state == "awaiting_query":
            query = message_text
            logger.info(f"Processing search query from user {user_id}: {query}")
            user_states[user_id].state = None
            user_states[user_id].query = query
            user_states[user_id].current_page = 0
            user_states[user_id].last_search_time = datetime.now()
            user_states[user_id].save_to_db()

            await cleanup_load_more_state(user_id, context)

            processing_message = await update.message.reply_text(
                LOCALES[lang]["searching"],
                reply_markup=ReplyKeyboardRemove(),
            )

            await send_paper_results(
                update, context, query, processing_message, lang=lang
            )
        else:
            query = message_text
            await cleanup_load_more_state(user_id, context)

            processing_message = await update.message.reply_text(
                LOCALES[lang]["searching"],
                reply_markup=ReplyKeyboardRemove(),
            )

            await send_paper_results(
                update, context, query, processing_message, lang=lang
            )
    except Exception as e:
        logger.exception(f"Error in handle_text: {e}")
        try:
            lang = detect(update.message.text)[:2] if update.message.text else "en"
            if lang not in LOCALES:
                lang = "en"
        except:
            lang = "en"
        await update.message.reply_text(
            LOCALES[lang]["error"],
            reply_markup=get_main_keyboard(),
        )


# Add logging to debug callback query handling
async def download_paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    chat_id = query.message.chat_id
    data = query.data

    logger.info(f"Download button clicked. Callback data: {data}")  # Debug log

    if user_id not in user_states or not user_states[user_id].query:
        await query.message.reply_text(
            "Your session has expired. Please start a new search.",
            reply_markup=get_main_keyboard(),
        )
        return

    try:
        paper_index = int(data.split("_")[-1])  # Extract paper index from callback data
    except ValueError:
        logger.error(f"Invalid callback data format: {data}")
        await query.message.reply_text(
            "Invalid request. Please try again.", reply_markup=get_main_keyboard()
        )
        return

    user_state = user_states[user_id]
    query_text = user_state.query

    # Fetch papers again to ensure we have the latest data
    result = search_arxiv(
        query_text, max_results=50
    )  # Fetch more results to cover all indices

    if isinstance(result, dict) and "error" in result:
        error_msg = result.get("message", "An unknown error occurred.")
        logger.error(f"Error fetching papers: {error_msg}")
        await query.message.reply_text(
            f"❌ {error_msg}", reply_markup=get_main_keyboard()
        )
        return

    papers = result
    if paper_index >= len(papers):
        logger.warning(
            f"Paper index {paper_index} out of range. Total papers: {len(papers)}"
        )
        await query.message.reply_text(
            "No paper found at the specified index.", reply_markup=get_main_keyboard()
        )
        return

    paper = papers[paper_index]
    pdf_url = paper["link"].replace(
        "abs", "pdf"
    )  # Convert arXiv abstract URL to PDF URL

    try:
        logger.info(f"Sending PDF: {pdf_url}")
        await context.bot.send_document(
            chat_id=chat_id,
            document=pdf_url,
            filename=f"{paper['title']}.pdf",
            caption=f"📄 {paper['title']}\n\n🔗 [Read more]({paper['link']})",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error sending PDF: {e}")
        await query.message.reply_text(
            "An error occurred while sending the PDF.", reply_markup=get_main_keyboard()
        )


async def send_paper_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    processing_message=None,
    is_load_more=False,
    lang="en",
):
    try:
        user_id = update.effective_user.id
        if user_id not in user_states:
            user_states[user_id] = UserState(user_id)

        user_state = user_states[user_id]

        if not is_load_more:
            user_state.current_page = 0
            user_state.query = query
            user_state.save_to_db()
            await cleanup_load_more_state(user_id, context)

        page = user_state.current_page
        results_per_page = user_state.results_per_page

        logger.info(f"Searching arXiv for: {query} (page {page+1})")

        # Fetch more results to anticipate future "Load More" clicks
        max_results = results_per_page * (page + 2)  # Increased to cover next page
        result = search_arxiv(query, max_results=max_results)

        if processing_message:
            try:
                await processing_message.delete()
            except Exception as e:
                logger.warning(f"Could not delete processing message: {e}")

        if isinstance(result, dict) and "error" in result:
            error_msg = result.get("message", "An unknown error occurred.")
            await update.effective_message.reply_text(
                f"❌ {error_msg}", reply_markup=get_main_keyboard()
            )
            return

        papers = result
        if not papers:
            await update.effective_message.reply_text(
                LOCALES[lang]["no_papers"],
                reply_markup=get_main_keyboard(),
            )
            return

        if is_load_more and page > 0:
            start_index = results_per_page * page
            if start_index >= len(papers):
                await update.effective_message.reply_text(
                    LOCALES[lang]["no_more_papers"],
                    reply_markup=get_main_keyboard(),
                )
                return
            papers_to_show = papers[start_index : start_index + results_per_page]
        else:
            papers_to_show = papers[:results_per_page]

        if not is_load_more:
            logger.info(f"Found {len(papers)} papers for query: {query}")
            await update.effective_message.reply_text(
                LOCALES[lang]["results_found"].format(count=len(papers))
            )
        else:
            logger.info(f"Loading more results for query: {query} (page {page+1})")

        for i, paper in enumerate(papers_to_show):
            try:
                msg = (
                    f"📄 *{paper['title']}*\n\n"
                    f"👤 Authors: {paper['authors']}\n\n"
                    f"📅 Published: {paper['published']}\n"
                    f"🏷️ Categories: {paper['categories']}\n\n"
                    f"{paper['summary']}\n\n"
                    f"🔗 [Read more]({paper['link']})"
                )

                # Add "Download PDF" button for each paper
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "📄 Download PDF", callback_data=f"download_{i}"
                        )
                    ]
                ]

                # Add "Load More" button on the last paper if more results exist
                if i == len(papers_to_show) - 1:
                    next_index = results_per_page * (page + 1)
                    has_more = next_index < len(papers)
                    logger.info(
                        f"has_more: {has_more}, next_index: {next_index}, total_papers: {len(papers)}"
                    )

                    if has_more:
                        keyboard.append(
                            [
                                InlineKeyboardButton(
                                    "📚 Load More Results", callback_data="load_more"
                                )
                            ]
                        )

                reply_markup = InlineKeyboardMarkup(keyboard)

                await update.effective_message.reply_markdown(
                    msg, reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Error sending paper {i+1}: {e}")
                continue
    except asyncio.CancelledError:
        logger.info("Paper search cancelled due to bot shutdown")
        if processing_message:
            try:
                await processing_message.delete()
            except Exception:
                pass
        await update.effective_message.reply_text(
            LOCALES[lang]["error"],
            reply_markup=get_main_keyboard(),
        )
    except Exception as e:
        logger.exception(f"Error in send_paper_results: {e}")
        if processing_message:
            try:
                await processing_message.delete()
            except Exception:
                pass
        await update.effective_message.reply_text(
            LOCALES[lang]["error"],
            reply_markup=get_main_keyboard(),
        )


if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(handle_load_more, pattern="^load_more$"))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_handler(CallbackQueryHandler(download_paper, pattern="^download_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info(f"Python version: {sys.version}")
    logger.info(f"PTB version: {telegram.__version__}")
    logger.info("Bot is now running and ready to receive messages")

    exit_code = 0
    shutdown_reason = "normal termination"

    logger.info("Starting bot polling...")
    try:
        app.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
        shutdown_reason = "keyboard interrupt"
    except SystemExit:
        logger.info("System exit received")
        shutdown_reason = "system exit"
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram API error: {e}")
        shutdown_reason = f"Telegram error: {type(e).__name__}"
        exit_code = 1
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
        shutdown_reason = f"network error: {type(e).__name__}"
        exit_code = 1
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        shutdown_reason = f"unhandled exception: {type(e).__name__}"
        exit_code = 1
    finally:
        logger.info(f"Beginning shutdown process (reason: {shutdown_reason})")
        try:
            logger.info("Performing final cleanup")
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")
            if exit_code == 0:
                exit_code = 1
        logger.info(f"Bot shutdown complete with exit code {exit_code}")

    if exit_code != 0:
        sys.exit(exit_code)
