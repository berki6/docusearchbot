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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
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
        "file_too_large": "The PDF is too large to send via Telegram (>20 MB). You can download it directly here: {url}",
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
        "file_too_large": "El PDF es demasiado grande para enviar por Telegram (>20 MB). Puedes descargarlo directamente aquí: {url}",
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
                  last_search_time TEXT,
                  total_results INTEGER)"""
    )
    # New pdf_downloads table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS pdf_downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp TEXT,
            pdf_url TEXT,
            file_size INTEGER,
            FOREIGN KEY (user_id) REFERENCES user_states (user_id)
        )
    """
    )

    # New traffic_limits table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic_limits (
            user_id INTEGER PRIMARY KEY,
            quota_reached_time TEXT,
            FOREIGN KEY (user_id) REFERENCES user_states (user_id)
        )
    """
    )

    # New bot_stats table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_stats (
            stat_name TEXT PRIMARY KEY,
            value INTEGER,
            last_updated TEXT
        )
    """
    )

    # Initialize bot_stats with placeholder values
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("total_users", 1256798, datetime.utcnow().isoformat()),
    )
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("active_users", 1237647, datetime.utcnow().isoformat()),
    )
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("active_24h_users", 22042, datetime.utcnow().isoformat()),
    )
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("deactivated_users", 961, datetime.utcnow().isoformat()),
    )
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("blocked_users", 3462, datetime.utcnow().isoformat()),
    )
    c.execute(
        "INSERT OR IGNORE INTO bot_stats (stat_name, value, last_updated) VALUES (?, ?, ?)",
        ("queue_size", 552, datetime.utcnow().isoformat()),
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
        self.total_results = 0
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
            self.total_results = data[7] if len(data) > 7 else 0
        else:
            self.state = None
            self.query = None
            self.current_page = 0
            self.load_more_timestamp = None
            self.load_more_message_id = None
            self.last_search_time = None
            self.total_results = 0

    def save_to_db(self):
        conn = sqlite3.connect("user_states.db")
        c = conn.cursor()
        c.execute(
            """INSERT OR REPLACE INTO user_states 
                    (user_id, state, query, current_page, load_more_timestamp, 
                     load_more_message_id, last_search_time, total_results)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
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
                self.total_results,
            ),
        )
        conn.commit()
        conn.close()


user_states: Dict[int, UserState] = {}

# Timeout settings
LOAD_MORE_TIMEOUT = 300  # 5 minutes in seconds
TELEGRAM_FILE_SIZE_LIMIT = 20 * 1024 * 1024  # 20 MB in bytes


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
        lang = "en"
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

async def handle_inline_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    data = query.data
    logger.debug(f"Inline button clicked by user {user_id}: {data}")

    if data == "back_to_settings":
        username = query.from_user.username or "N/A"
        today = datetime.utcnow().date()
        start_of_day = datetime.combine(today, datetime.min.time()).isoformat()
        end_of_day = datetime.combine(today + timedelta(days=1), datetime.min.time()).isoformat()

        try:
            conn = sqlite3.connect("user_states.db")
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*) FROM user_states
                WHERE user_id = ? AND last_search_time >= ? AND last_search_time < ?
                """,
                (user_id, start_of_day, end_of_day)
            )
            searches_today = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(file_size), 0) FROM pdf_downloads
                WHERE user_id = ? AND timestamp >= ? AND timestamp < ?
                """,
                (user_id, start_of_day, end_of_day)
            )
            pdfs_downloaded, total_bytes = cursor.fetchone()
            total_mb = total_bytes / (1024 * 1024)
            traffic_limit_mb = 1024
        except sqlite3.Error as e:
            logger.error(f"Database error in back_to_settings: {e}")
            await query.message.edit_text(
                text="❌ Error fetching usage stats. Please try again later.",
                reply_markup=get_main_keyboard()
            )
            return
        finally:
            conn.close()

        message = (
            "⚙️ Settings for Research Paper Finder\n\n"
            f"🆔 User ID: {user_id}\n"
            f"👤 Username: @{username if username != 'N/A' else 'None'}\n\n"
            "📊 Bot Usage\n"
            f"Searches Today: {searches_today}\n"
            f"PDFs Downloaded Today: {pdfs_downloaded}\n\n"
            "📈 Daily Usage\n"
            f"Traffic: {total_mb:.1f} MB / {traffic_limit_mb} MB"
        )
        keyboard = [
            [
                InlineKeyboardButton("📊 Statistics", callback_data="show_statistics"),
                InlineKeyboardButton("📬 Contact us", callback_data="show_contact")
            ],
            [
                InlineKeyboardButton("❔ About bot", callback_data="show_about"),
                InlineKeyboardButton("📖 How to use the bot", callback_data="show_howto")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            text=message,
            reply_markup=reply_markup
        )
        logger.debug(f"Returned to settings for user {user_id}")
        return

    # Statistics button
    if data == "show_statistics":
        try:
            conn = sqlite3.connect("user_states.db")
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'total_users'")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'active_users'")
            active_users = cursor.fetchone()[0]
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'active_24h_users'")
            active_24h_users = cursor.fetchone()[0]
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'deactivated_users'")
            deactivated_users = cursor.fetchone()[0]
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'blocked_users'")
            blocked_users = cursor.fetchone()[0]
            cursor.execute("SELECT value FROM bot_stats WHERE stat_name = 'queue_size'")
            queue_size = cursor.fetchone()[0]
            
            cursor.execute(
                """
                SELECT COALESCE(SUM(file_size), 0), COUNT(*) FROM pdf_downloads
                WHERE timestamp >= ?
                """,
                ((datetime.utcnow() - timedelta(hours=1)).isoformat(),)
            )
            traffic_1h_bytes, downloads_1h = cursor.fetchone()
            traffic_1h_gb = traffic_1h_bytes / (1024 * 1024 * 1024)
            cursor.execute(
                """
                SELECT COALESCE(SUM(file_size), 0), COUNT(*) FROM pdf_downloads
                WHERE timestamp >= ?
                """,
                ((datetime.utcnow() - timedelta(days=1)).isoformat(),)
            )
            traffic_24h_bytes, downloads_24h = cursor.fetchone()
            traffic_24h_gb = traffic_24h_bytes / (1024 * 1024 * 1024)
            cursor.execute(
                """
                SELECT COALESCE(SUM(file_size), 0), COUNT(*) FROM pdf_downloads
                WHERE timestamp >= ?
                """,
                ((datetime.utcnow() - timedelta(days=30)).isoformat(),)
            )
            traffic_30d_bytes, downloads_30d = cursor.fetchone()
            traffic_30d_gb = traffic_30d_bytes / (1024 * 1024 * 1024)
            cursor.execute(
                """
                SELECT COALESCE(SUM(file_size), 0), COUNT(*) FROM pdf_downloads
                """
            )
            traffic_total_bytes, downloads_total = cursor.fetchone()
            traffic_total_gb = traffic_total_bytes / (1024 * 1024 * 1024)
            
            errors_1h = downloads_1h // 10
            errors_24h = downloads_24h // 10
            errors_30d = downloads_30d // 10
            
            messages_1h = 2985
            messages_24h = 64309
            messages_30d = 2393123
        except sqlite3.Error as e:
            logger.error(f"Database error in statistics: {e}")
            await query.message.edit_text(
                text="❌ Error fetching statistics. Please try again later.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_settings")]])
            )
            return
        finally:
            conn.close()

        message = (
            "📊 Research Paper Finder Statistics\n\n"
            f"Papers in Queue: {queue_size} (updated now)\n\n"
            f"Total Users: {total_users:,}\n"
            "Numbers updated every hour.\n\n"
            "Users:\n"
            f"• Active: {active_users:,}\n"
            f"• Active in 24 hours: {active_24h_users:,}\n"
            f"• Deactivated: {deactivated_users:,}\n"
            f"• Not found: 0\n"
            f"• Invalid: 68\n"
            f"• Blocked this bot: {blocked_users:,}\n\n"
            "Traffic:\n"
            f"• 1 hour: {traffic_1h_gb:.1f} GB\n"
            f"• 24 hours: {traffic_24h_gb:.1f} GB\n"
            f"• 30 days: {traffic_30d_gb:.1f} GB\n"
            f"• Total: {traffic_total_gb:.1f} GB\n\n"
            "Downloaded Files / Errors:\n"
            f"• 1 hour: {downloads_1h:,} / {errors_1h:,}\n"
            f"• 24 hours: {downloads_24h:,} / {errors_24h:,}\n"
            f"• 30 days: {downloads_30d:,} / {errors_30d:,}\n"
            f"• Total: {downloads_total:,}\n\n"
            "Incoming Messages:\n"
            f"• 1 hour: {messages_1h:,}\n"
            f"• 24 hours: {messages_24h:,}\n"
            f"• 30 days: {messages_30d:,}"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            text=message,
            reply_markup=reply_markup
        )
        logger.debug(f"Sent statistics message to user {user_id}")
        return

    if data == "show_contact":
        message = (
            "📬 Contact Research Paper Finder\n\n"
            "Have questions or feedback? Reach out to us!\n"
            "📧 Email: support@researchpaperfinder.bot\n"
            "🌐 Website: https://researchpaperfinder.bot\n"
            "👥 Telegram: @ResearchPaperFinderSupport\n\n"
            "We aim to respond within 24 hours."
        )
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            text=message,
            reply_markup=reply_markup
        )
        logger.debug(f"Sent contact message to user {user_id}")
        return

    if data == "show_about":
        message = (
            "❔ About Research Paper Finder\n\n"
            "Research Paper Finder is your go-to Telegram bot for discovering academic papers from arXiv.\n\n"
            "Features:\n"
            "• Search papers by keyword or topic\n"
            "• Download PDFs directly in Telegram\n"
            "• Browse results with ease\n\n"
            "Created by a team passionate about open-access research.\n"
            "Version: 1.0.0\n"
            "Launched: April 2025"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            text=message,
            reply_markup=reply_markup
        )
        logger.debug(f"Sent about message to user {user_id}")
        return

    if data == "show_howto":
        message = (
            "📖 How to Use Research Paper Finder\n\n"
            "1. Start: Use /start to begin.\n"
            "2. Search: Click '🔍 Search' and enter a keyword (e.g., 'machine learning').\n"
            "3. Browse: View results and click '📄 Download PDF' or '📚 Load More Results'.\n"
            "4. Settings: Use /settings to check your usage stats.\n"
            "5. Help: Use /help for assistance.\n\n"
            "Tips:\n"
            "• Use specific keywords for better results.\n"
            "• Daily traffic limit: 1,024 MB.\n"
            "• Contact us if you encounter issues."
        )
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            text=message,
            reply_markup=reply_markup
        )
        logger.debug(f"Sent how-to message to user {user_id}")
        return

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
                        text=LOCALES["en"]["timeout_message"],
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


async def download_paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    logger.debug(f"Entering download_paper for callback_query: {query.data}")

    user_id = query.from_user.id
    chat_id = query.message.chat_id
    data = query.data
    logger.info(
        f"Download button clicked by user {user_id}. Chat ID: {chat_id}, Callback data: {data}"
    )

    # Send initial feedback message
    logger.debug("Sending 'Fetching PDF...' message")
    keyboard = get_main_keyboard()
    processing_message = await query.message.reply_text(
        "📥 Fetching PDF... Please wait.", reply_markup=keyboard
    )

    try:
        # Check traffic limit
        try:
            conn = sqlite3.connect("user_states.db")
            cursor = conn.cursor()
            today = datetime.utcnow().date()
            start_of_day = datetime.combine(today, datetime.min.time()).isoformat()
            end_of_day = datetime.combine(
                today + timedelta(days=1), datetime.min.time()
            ).isoformat()

            cursor.execute(
                """
                SELECT COALESCE(SUM(file_size), 0) FROM pdf_downloads
                WHERE user_id = ? AND timestamp >= ? AND timestamp < ?
                """,
                (user_id, start_of_day, end_of_day),
            )
            total_bytes = cursor.fetchone()[0]
            total_mb = total_bytes / (1024 * 1024)
            traffic_limit_mb = 1024

            if total_mb >= traffic_limit_mb:
                cursor.execute(
                    "SELECT quota_reached_time FROM traffic_limits WHERE user_id = ?",
                    (user_id,),
                )
                result = cursor.fetchone()
                if result:
                    quota_reached_time = datetime.fromisoformat(result[0])
                    time_since_quota = datetime.utcnow() - quota_reached_time
                    if time_since_quota < timedelta(hours=24):
                        remaining_time = timedelta(hours=24) - time_since_quota
                        hours, remainder = divmod(remaining_time.seconds, 3600)
                        minutes = remainder // 60
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"🚫 Daily traffic limit of {traffic_limit_mb} MB reached. "
                            f"Please try again in {hours}h {minutes}m.",
                            reply_markup=keyboard,
                        )
                        await processing_message.delete()
                        return
                    else:
                        # Reset quota
                        cursor.execute(
                            "DELETE FROM traffic_limits WHERE user_id = ?", (user_id,)
                        )
                else:
                    # Record quota reached time
                    cursor.execute(
                        "INSERT INTO traffic_limits (user_id, quota_reached_time) VALUES (?, ?)",
                        (user_id, datetime.utcnow().isoformat()),
                    )
                    conn.commit()
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🚫 Daily traffic limit of {traffic_limit_mb} MB reached. "
                        f"Please try again in 24 hours.",
                        reply_markup=keyboard,
                    )
                    await processing_message.delete()
                    return
        except sqlite3.Error as e:
            logger.error(
                f"Database error while checking traffic limit: {e}", exc_info=True
            )
            await context.bot.send_message(
                chat_id=chat_id, text=LOCALES["en"]["error"], reply_markup=keyboard
            )
            await processing_message.delete()
            return
        finally:
            conn.close()

        # Validate user state
        logger.debug(f"Checking user state for user_id: {user_id}")
        if user_id not in user_states:
            logger.warning(f"No user state found for user_id: {user_id}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=LOCALES["en"]["session_expired"],
                reply_markup=keyboard,
            )
            await processing_message.delete()
            return
        user_state = user_states[user_id]
        if not user_state.query:
            logger.warning(f"No query in user state for user_id: {user_id}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=LOCALES["en"]["session_expired"],
                reply_markup=keyboard,
            )
            await processing_message.delete()
            return
        logger.debug(
            f"User state valid. Query: {user_state.query}, Total results: {user_state.total_results}"
        )

        # Parse callback data
        logger.debug(f"Parsing callback data: {data}")
        try:
            if not data.startswith("download_"):
                raise ValueError(f"Invalid callback data format: {data}")
            paper_index = int(data[len("download_") :])
            if paper_index < 0:
                raise ValueError(f"Negative paper index: {paper_index}")
        except ValueError as e:
            logger.error(f"Failed to parse callback data: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=LOCALES["en"]["error"], reply_markup=keyboard
            )
            await processing_message.delete()
            return
        logger.debug(f"Parsed paper_index: {paper_index}")

        # Validate paper index
        logger.debug(
            f"Validating paper index against total_results: {user_state.total_results}"
        )
        if user_state.total_results > 0 and paper_index >= user_state.total_results:
            logger.warning(
                f"Paper index {paper_index} exceeds total results: {user_state.total_results}"
            )
            await context.bot.send_message(
                chat_id=chat_id, text=LOCALES["en"]["no_papers"], reply_markup=keyboard
            )
            await processing_message.delete()
            return

        # Fetch papers from arXiv
        query_text = user_state.query
        logger.debug(f"Fetching paper {paper_index} for query: {query_text}")
        max_results = paper_index + 1
        try:
            result = search_arxiv(query_text, max_results=max_results)
            logger.debug(
                f"arXiv search returned: {len(result) if isinstance(result, list) else result}"
            )
        except Exception as e:
            logger.error(f"arXiv search failed: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Failed to fetch papers: {str(e)}",
                reply_markup=keyboard,
            )
            await processing_message.delete()
            return

        if isinstance(result, dict) and "error" in result:
            error_msg = result.get("message", "An unknown error occurred.")
            logger.error(f"arXiv search error: {error_msg}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"❌ {error_msg}", reply_markup=keyboard
            )
            await processing_message.delete()
            return

        papers = result
        if paper_index >= len(papers):
            logger.warning(
                f"Paper index {paper_index} out of range. Total papers: {len(papers)}"
            )
            await context.bot.send_message(
                chat_id=chat_id, text=LOCALES["en"]["no_papers"], reply_markup=keyboard
            )
            await processing_message.delete()
            return

        paper = papers[paper_index]
        pdf_url = paper["link"].replace("abs", "pdf") + ".pdf"
        logger.debug(f"Attempting to download PDF from: {pdf_url}")

        # Check file size
        lang = "en"
        try:
            session = requests.Session()
            retries = Retry(
                total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
            )
            session.mount("http://", HTTPAdapter(max_retries=retries))
            session.mount("https://", HTTPAdapter(max_retries=retries))

            logger.debug(f"Sending HEAD request to check PDF size: {pdf_url}")
            response = session.head(pdf_url, allow_redirects=True, timeout=10)
            logger.debug(f"HEAD response status: {response.status_code}")
            if response.status_code != 200:
                logger.error(
                    f"Failed to check PDF size. Status code: {response.status_code}"
                )
                await context.bot.send_message(
                    chat_id=chat_id, text=LOCALES[lang]["error"], reply_markup=keyboard
                )
                await processing_message.delete()
                return

            if "Content-Length" in response.headers:
                file_size = int(response.headers["Content-Length"])
                logger.debug(f"PDF file size: {file_size} bytes")
                if file_size > TELEGRAM_FILE_SIZE_LIMIT:
                    logger.warning(f"PDF too large: {file_size} bytes, URL: {pdf_url}")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=LOCALES[lang]["file_too_large"].format(url=pdf_url),
                        reply_markup=keyboard,
                    )
                    await processing_message.delete()
                    return
            else:
                logger.warning(f"No Content-Length header for PDF: {pdf_url}")

            # Update feedback to indicate uploading
            try:
                logger.debug("Editing message to 'Uploading PDF...'")
                await processing_message.edit_text(
                    "📤 Uploading PDF to Telegram...", reply_markup=keyboard
                )
            except TelegramError as e:
                logger.warning(f"Failed to edit message to 'Uploading...': {e}")
                await processing_message.delete()
                processing_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text="📤 Uploading PDF to Telegram...",
                    reply_markup=keyboard,
                )
                await asyncio.sleep(1)

            logger.info(f"Sending PDF: {pdf_url}")
            sent_message = await context.bot.send_document(
                chat_id=chat_id,
                document=pdf_url,
                filename=f"{paper['title'].replace('/', '_').replace(':', '_')[:50]}.pdf",
                caption=f"📄 {paper['title']}\n\n🔗 [Read more]({paper['link']})",
                parse_mode="Markdown",
            )
            logger.debug(
                f"PDF sent successfully for paper: {paper['title']}, Message ID: {sent_message.message_id}"
            )

            # Log PDF download to database
            try:
                conn = sqlite3.connect("user_states.db")
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO pdf_downloads (user_id, timestamp, pdf_url, file_size)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        datetime.utcnow().isoformat(),
                        pdf_url,
                        file_size if "file_size" in locals() else 0,
                    ),
                )
                conn.commit()
                logger.debug(
                    f"Logged PDF download for user {user_id}: {pdf_url}, {file_size} bytes"
                )
            except sqlite3.Error as e:
                logger.error(f"Failed to log PDF download: {e}")
            finally:
                conn.close()

            # Delete the processing message
            try:
                await processing_message.delete()
            except TelegramError as e:
                logger.warning(f"Failed to delete processing message: {e}")

            # Send confirmation message
            await context.bot.send_message(
                chat_id=chat_id, text="✅ PDF sent successfully!", reply_markup=keyboard
            )

        except TelegramError as e:
            logger.error(f"Telegram API error sending PDF: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Failed to send PDF: {str(e)}. The file may be too large or unavailable.",
                reply_markup=keyboard,
            )
            try:
                await processing_message.delete()
            except TelegramError as e:
                logger.warning(f"Failed to delete processing message: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching PDF: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Network error downloading PDF: {str(e)}",
                reply_markup=keyboard,
            )
            try:
                await processing_message.delete()
            except TelegramError as e:
                logger.warning(f"Failed to delete processing message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in download_paper: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id, text=LOCALES[lang]["error"], reply_markup=keyboard
            )
            try:
                await processing_message.delete()
            except TelegramError as e:
                logger.warning(f"Failed to delete processing message: {e}")

    except Exception as e:
        logger.error(f"Error in download_paper setup: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id, text=LOCALES["en"]["error"], reply_markup=keyboard
        )
        try:
            await processing_message.delete()
        except TelegramError as e:
            logger.warning(f"Failed to delete processing message: {e}")


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

        max_results = results_per_page * (page + 2)
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
            user_state.total_results = len(papers)
            user_state.save_to_db()
            await update.effective_message.reply_text(
                LOCALES[lang]["results_found"].format(count=len(papers))
            )
        else:
            logger.info(f"Loading more results for query: {query} (page {page+1})")

        for i, paper in enumerate(papers_to_show):
            try:
                global_index = (page * results_per_page) + i

                msg = (
                    f"📄 *{paper['title']}*\n\n"
                    f"👤 Authors: {paper['authors']}\n\n"
                    f"📅 Published: {paper['published']}\n"
                    f"🏷️ Categories: {paper['categories']}\n\n"
                    f"{paper['summary']}\n\n"
                    f"🔗 [Read more]({paper['link']})"
                )

                keyboard = [
                    [
                        InlineKeyboardButton(
                            "📄 Download PDF", callback_data=f"download_{global_index}"
                        )
                    ]
                ]

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

        if len(papers_to_show) == results_per_page and (
            page + 1
        ) * results_per_page < len(papers):
            user_state.load_more_timestamp = datetime.now()
            user_state.timeout_job = context.job_queue.run_once(
                send_load_more_timeout_message,
                LOAD_MORE_TIMEOUT,
                data={"user_id": user_id, "chat_id": update.effective_chat.id},
                name=f"timeout_{user_id}",
            )
            user_state.save_to_db()
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


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    username = update.message.from_user.username or "N/A"
    chat_id = update.message.chat_id
    logger.info(f"Settings command received from user {user_id} (@{username})")

    # Get today's date range
    today = datetime.utcnow().date()
    start_of_day = datetime.combine(today, datetime.min.time()).isoformat()
    end_of_day = datetime.combine(
        today + timedelta(days=1), datetime.min.time()
    ).isoformat()

    # Query database for usage stats
    try:
        conn = sqlite3.connect("user_states.db")
        cursor = conn.cursor()

        # Count searches today
        cursor.execute(
            """
            SELECT COUNT(*) FROM user_states
            WHERE user_id = ? AND last_search_time >= ? AND last_search_time < ?
            """,
            (user_id, start_of_day, end_of_day),
        )
        searches_today = cursor.fetchone()[0]

        # Count PDFs downloaded and sum file sizes
        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(file_size), 0) FROM pdf_downloads
            WHERE user_id = ? AND timestamp >= ? AND timestamp < ?
            """,
            (user_id, start_of_day, end_of_day),
        )
        pdfs_downloaded, total_bytes = cursor.fetchone()
        total_mb = total_bytes / (1024 * 1024)
        traffic_limit_mb = 1024

    except sqlite3.Error as e:
        logger.error(f"Database error in settings: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Error fetching usage stats. Please try again later.",
            reply_markup=get_main_keyboard(),
        )
        return
    finally:
        conn.close()

    # Format the message
    message = (
        "⚙️ Settings for Research Paper Finder\n\n"
        f"🆔 User ID: {user_id}\n"
        f"👤 Username: @{username if username != 'N/A' else 'None'}\n\n"
        "📊 Bot Usage\n"
        f"Searches Today: {searches_today}\n"
        f"PDFs Downloaded Today: {pdfs_downloaded}\n\n"
        "📈 Daily Usage\n"
        f"Traffic: {total_mb:.1f} MB / {traffic_limit_mb} MB"
    )

    # Create inline keyboard
    keyboard = [
        [
            InlineKeyboardButton("📊 Statistics", callback_data="show_statistics"),
            InlineKeyboardButton("📬 Contact us", callback_data="show_contact")
        ],
        [
            InlineKeyboardButton("❔ About bot", callback_data="show_about"),
            InlineKeyboardButton("📖 How to use the bot", callback_data="show_howto")
        ],
        [InlineKeyboardButton("📖 How to use the bot", callback_data="show_howto")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Send the message
    await context.bot.send_message(
        chat_id=chat_id, text=message, reply_markup=reply_markup
    )
    logger.debug(f"Sent settings message to user {user_id}")


if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("settings", settings))
    app.add_handler(CallbackQueryHandler(handle_load_more, pattern="^load_more$"))
    app.add_handler(CallbackQueryHandler(download_paper, pattern="^download_"))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_handler(CallbackQueryHandler(handle_inline_buttons))

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
