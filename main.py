import os
import sys
import time
import signal
import signal
import logging
import asyncio
import requests
import enum
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pytz import utc
import telegram
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
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
# Remove hardcoded token for security
user_states = {}  # Track user states for input


# --- Progress tracking enum ---
class SearchProgress(enum.Enum):
    INITIALIZING = (0, "🔍 Initializing search...")
    CONNECTING = (20, "🔌 Connecting to arXiv API...")
    REQUESTING = (40, "📡 Sending request to arXiv...")
    RECEIVING = (60, "📥 Receiving data from arXiv...")
    PROCESSING = (80, "⚙️ Processing search results...")
    FINALIZING = (95, "📊 Finalizing results...")
    COMPLETE = (100, "✅ Search complete!")
    ERROR_TIMEOUT = (-1, "⚠️ Connection timed out. Retrying...")
    ERROR_CONNECTION = (-2, "❌ Connection error with arXiv API.")
    ERROR_HTTP = (-3, "⚠️ HTTP error from arXiv API.")
    ERROR_OTHER = (-4, "⚠️ Error during search.")

    def __init__(self, percentage, message):
        self.percentage = percentage
        self.message = message


# --- Helper to fetch from arXiv ---
def search_arxiv(query: str, max_results=5, progress_callback=None):
    try:
        # Signal initial progress
        if progress_callback:
            progress_callback(SearchProgress.INITIALIZING)
            
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # Increased backoff factor for exponential delays
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=True,
            respect_retry_after_header=True,
            # Retry on connection errors and read timeouts
            connect=3,
            read=3
        )

        # Configure connection pooling and reuse settings
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
            pool_block=False
        )
        session.mount("https://", adapter)

        url = f"https://export.arxiv.org/api/query?search_query=all:{query}&start=0&max_results={max_results}"
        logger.info("Requesting arXiv API with extended timeout and retry mechanism")

        # Signal connecting progress
        if progress_callback:
            progress_callback(SearchProgress.CONNECTING)

        # Signal requesting progress
        if progress_callback:
            progress_callback(SearchProgress.REQUESTING)
            
        response = session.get(url, timeout=60)  # Increased timeout to 60 seconds
        
        # Signal receiving progress
        if progress_callback:
            progress_callback(SearchProgress.RECEIVING)

        entries = []

        if response.status_code == 200:
            from xml.etree import ElementTree as ET

            try:
                # Signal processing progress
                if progress_callback:
                    progress_callback(SearchProgress.PROCESSING)
                    
                root = ET.fromstring(response.content)
                namespace = "{http://www.w3.org/2005/Atom}"

                all_entries = root.findall(f"{namespace}entry")

                for entry in all_entries:
                    try:
                        title_elem = entry.find(f"{namespace}title")
                        link_elem = entry.find(f"{namespace}id")
                        summary_elem = entry.find(f"{namespace}summary")

                        if title_elem is None or link_elem is None:
                            logger.warning("Missing required elements in entry")
                            continue

                        title = title_elem.text.strip()
                        link = link_elem.text.strip()
                        summary = ""
                        if summary_elem is not None and summary_elem.text:
                            summary = summary_elem.text.strip()[:500] + "..."
                        else:
                            summary = "No summary available"

                        entries.append(
                            {"title": title, "link": link, "summary": summary}
                        )
                    except Exception as e:
                        logger.error(f"Error processing entry: {e}")
                        continue
            except ET.ParseError as e:
                logger.error(f"Error parsing XML response: {e}")
                # Signal error progress
                if progress_callback:
                    progress_callback(SearchProgress.ERROR_OTHER)
        else:
            logger.error(f"Bad status code: {response.status_code}")
            # Signal HTTP error progress
            if progress_callback:
                progress_callback(SearchProgress.ERROR_HTTP)

    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error when accessing arXiv API: {e}")
        # Signal timeout error progress
        if progress_callback:
            progress_callback(SearchProgress.ERROR_TIMEOUT)
        return {
            "error": "timeout",
            "message": "The request to arXiv timed out. Please try again later.",
        }
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error when accessing arXiv API: {e}")
        # Signal connection error progress
        if progress_callback:
            progress_callback(SearchProgress.ERROR_CONNECTION)
        return {
            "error": "connection",
            "message": "Could not connect to arXiv. Please check your internet connection and try again.",
        }
    except requests.exceptions.HTTPError as e:
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response
            else "unknown"
        )
        logger.error(
            f"HTTP error when accessing arXiv API: {e} (Status code: {status_code})"
        )
        return {
            "error": "http",
            "message": f"Received HTTP error {status_code} from arXiv. The service might be temporarily unavailable.",
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception when accessing arXiv API: {e}")
        # Signal other error progress
        if progress_callback:
            progress_callback(SearchProgress.ERROR_OTHER)
        return {
            "error": "request",
            "message": "An error occurred while communicating with arXiv. Please try again later.",
        }
    except Exception as e:
        logger.exception(f"Error in search_arxiv: {e}")
        # Signal other error progress
        if progress_callback:
            progress_callback(SearchProgress.ERROR_OTHER)
        return {
            "error": "unknown",
            "message": "An unexpected error occurred. Please try again later.",
        }
    finally:
        # Signal completion regardless of success or failure
        if progress_callback:
            progress_callback(SearchProgress.FINALIZING)


# --- Helper function for keyboard ---
def get_main_keyboard():
    """Returns the main keyboard with Search and Help buttons"""
    keyboard = [[KeyboardButton(text="🔍 Search")], [KeyboardButton(text="📖 Help")]]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,  # Make buttons smaller
        one_time_keyboard=False,  # Keep keyboard visible
    )


# --- Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = get_main_keyboard()
    await update.message.reply_text(
        "📚 Welcome to Research Paper Bot! Choose an option:", reply_markup=reply_markup
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Use the 🔍 Search button or type a topic directly to find academic papers from arXiv.",
        reply_markup=get_main_keyboard(),
    )


async def handle_message_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    user_id = update.message.from_user.id

    if message_text == "🔍 Search":
        user_states[user_id] = "awaiting_query"
        # Remove keyboard while waiting for search input
        await update.message.reply_text(
            "Please enter your search keywords (e.g., deep learning):",
            reply_markup=ReplyKeyboardRemove(),
        )

    elif message_text == "📖 Help":
        await update.message.reply_text(
            "Type a topic or use the search button to find papers. We'll fetch from arXiv.org.",
            reply_markup=get_main_keyboard(),
        )


# Keep the old handler for backward compatibility with existing inline buttons
async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "action_search":
        user_id = query.from_user.id
        user_states[user_id] = "awaiting_query"
        await query.message.reply_text(
            "Please enter your search keywords (e.g., deep learning):",
            reply_markup=ReplyKeyboardRemove(),
        )

    elif data == "action_help":
        await query.message.reply_text(
            "Type a topic or use the search button to find papers. We'll fetch from arXiv.org.",
            reply_markup=get_main_keyboard(),
        )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        message_text = update.message.text

        # Check if this is a button press
        if message_text in ["🔍 Search", "📖 Help"]:
            return await handle_message_buttons(update, context)

        if user_states.get(user_id) == "awaiting_query":
            query = message_text
            logger.info(f"Processing search query from user {user_id}: {query}")
            user_states[user_id] = None

            # Send a searching message to provide immediate feedback (with keyboard removed)
            processing_message = await update.message.reply_text(
                "🔍 Searching for papers... Please wait.",
                reply_markup=ReplyKeyboardRemove(),
            )

            # Process the search query
            await send_paper_results(update, context, query, processing_message)
        else:
            # If user just sent text without being in search mode, treat it as a search
            query = message_text

            # Send a searching message (with keyboard removed)
            processing_message = await update.message.reply_text(
                "🔍 Searching for papers... Please wait.",
                reply_markup=ReplyKeyboardRemove(),
            )

            await send_paper_results(update, context, query, processing_message)
    except Exception as e:
        logger.exception(f"Error in handle_text: {e}")
        # Restore keyboard even on error
        await update.message.reply_text(
            "Sorry, an error occurred while processing your message. Please try again.",
            reply_markup=get_main_keyboard(),
        )


# Create a progress tracker class to hold state
class ProgressTracker:
    def __init__(self, processing_message):
        self.processing_message = processing_message
        self.last_percentage = 0
        self.last_update_time = time.time()
        self.update_queue = asyncio.Queue()
        self.is_running = True

    async def update_progress(self, progress_state):
        # Add progress state to queue
        await self.update_queue.put(progress_state)
        
    async def run_updates(self):
        try:
            while self.is_running:
                # Get the next progress state or wait for one
                try:
                    progress_state = await asyncio.wait_for(self.update_queue.get(), 0.2)
                    
                    # Only update if enough time has passed (rate limiting) or it's an important update
                    current_time = time.time()
                    is_error = progress_state.percentage < 0
                    is_significant_change = abs(progress_state.percentage - self.last_percentage) >= 10
                    
                    if (current_time - self.last_update_time >= 0.5) or is_error or is_significant_change:
                        try:
                            if self.processing_message:
                                # Format percentage string only for positive percentages
                                percentage_str = f" ({progress_state.percentage}%)" if progress_state.percentage >= 0 else ""
                                await self.processing_message.edit_text(
                                    text=f"{progress_state.message}{percentage_str}"
                                )
                                self.last_percentage = progress_state.percentage
                                self.last_update_time = current_time
                        except telegram.error.BadRequest as e:
                            logger.warning(f"Failed to update progress: {e}")
                            self.is_running = False
                            break
                        
                    # Mark task as done
                    self.update_queue.task_done()
                    
                    # If we're complete, exit the loop
                    if progress_state == SearchProgress.COMPLETE:
                        self.is_running = False
                        break
                        
                except asyncio.TimeoutError:
                    # Just continue the loop if no update received
                    pass
                    
        except asyncio.CancelledError:
            logger.info("Progress updates cancelled")
            self.is_running = False
        except Exception as e:
            logger.exception(f"Error in progress updates: {e}")
            self.is_running = False


async def send_paper_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    processing_message=None,
):
    try:
        logger.info(f"Searching arXiv for: {query}")

        # Create a progress tracker
        progress_tracker = ProgressTracker(processing_message)
        
        # Start the progress updater task
        progress_task = asyncio.create_task(
            progress_tracker.run_updates()
        )
        
        # Define the callback for search_arxiv
        async def progress_callback(progress_state):
            await progress_tracker.update_progress(progress_state)

        # Perform the search
        # Run the search with the progress callback
        # We need to run this in a separate thread to not block the asyncio event loop
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: search_arxiv(
                query,
                progress_callback=lambda state: asyncio.run_coroutine_threadsafe(
                    progress_callback(state), loop
                )
            )
        )
        
        # Signal completion and cancel the progress task if needed
        await progress_tracker.update_progress(SearchProgress.COMPLETE)
        
        # Wait a moment for the complete message to be seen
        await asyncio.sleep(0.5)
        
        # Cancel the progress task if it hasn't completed
        if not progress_task.done():
            progress_task.cancel()

        # Delete the processing message if it exists
        if processing_message:
            try:
                await processing_message.delete()
            except Exception as e:
                logger.warning(f"Could not delete processing message: {e}")

        # Check if the result is an error
        if isinstance(result, dict) and "error" in result:
            error_msg = result.get("message", "An unknown error occurred.")
            await update.message.reply_text(
                f"❌ {error_msg}", reply_markup=get_main_keyboard()
            )
            return

        # Now we know result is a list of papers
        papers = result

        if not papers:
            await update.message.reply_text(
                "No papers found. Please try a different search term.",
                reply_markup=get_main_keyboard(),
            )
            return

        logger.info(f"Found {len(papers)} papers for query: {query}")
        await update.message.reply_text(
            f"📚 Found {len(papers)} papers matching your search."
        )

        for i, paper in enumerate(papers):
            try:
                msg = f"📄 *{paper['title']}*\n\n{paper['summary']}\n\n🔗 [Read more]({paper['link']})"

                # For the last paper, restore the keyboard
                if i == len(papers) - 1:
                    await update.message.reply_markdown(
                        msg, reply_markup=get_main_keyboard()
                    )
                else:
                    await update.message.reply_markdown(msg)
            except Exception as e:
                logger.error(f"Error sending paper {i+1}: {e}")
                continue
    except asyncio.CancelledError:
        # Handle the case where the task is cancelled (e.g., during bot shutdown)
        logger.info("Paper search cancelled due to bot shutdown")
        if processing_message:
            try:
                await processing_message.delete()
            except Exception:
                pass

        await update.message.reply_text(
            "Search operation was cancelled. Please try again later.",
            reply_markup=get_main_keyboard(),
        )
    except Exception as e:
        logger.exception(f"Error in send_paper_results: {e}")
        # Always make sure the keyboard is restored on error
        if processing_message:
            try:
                await processing_message.delete()
            except Exception:
                pass

        await update.message.reply_text(
            "Sorry, an error occurred while searching for papers. Please try again later.",
            reply_markup=get_main_keyboard(),
        )


# --- Main ---
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOTAPI environment variable not set")

    # Build application with default settings, letting python-telegram-bot
    # handle its internal job queue implementation
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Example of scheduling a task (commented out)
    # async def scheduled_task(context):
    #     """Example function that would be scheduled to run periodically"""
    #     print(f"Running scheduled task at {datetime.now()}")
    #     # You could send messages to users, update data, etc.
    #
    # # Schedule the task to run every 24 hours
    # app.job_queue.run_repeating(scheduled_task, interval=timedelta(hours=24), first=10)
    #
    # # Or schedule a one-time task
    # app.job_queue.run_once(scheduled_task, when=timedelta(minutes=5))

    # Add your handlers
    # Register command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    # Register message handler for text messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    logger.info(f"Python version: {sys.version}")
    logger.info(f"PTB version: {telegram.__version__}")
    logger.info("Bot is now running and ready to receive messages")

    # Simplified run approach with proper error handling
    exit_code = 0
    shutdown_reason = "normal termination"

    logger.info("Starting bot polling...")
    try:
        # Let python-telegram-bot handle its own signal handling
        # This works well on both Windows and Unix systems
        app.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
        shutdown_reason = "keyboard interrupt"
    except SystemExit:
        # Clean exit, just pass through
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
        # Log detailed shutdown information
        logger.info(f"Beginning shutdown process (reason: {shutdown_reason})")

        # Perform any needed cleanup operations here
        try:
            # Release any resources if needed
            logger.info("Performing final cleanup")
            # Clean up any custom resources here if needed
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")
            if exit_code == 0:  # Only set exit code if not already set
                exit_code = 1

        # Final shutdown message
        logger.info(f"Bot shutdown complete with exit code {exit_code}")

    # Only exit with error code if there was an actual error
    if exit_code != 0:
        sys.exit(exit_code)
