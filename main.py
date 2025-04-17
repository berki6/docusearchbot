import os
import sys
import time
import signal
import logging
import asyncio
import requests
import arxiv
import urllib3
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pytz import utc

import telegram
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
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

class UserState:
    """Class to track user state including search query, page, and timestamps"""
    def __init__(self):
        self.state = None  # Current state (e.g., "awaiting_query")
        self.query = None  # Last search query
        self.current_page = 0  # Current page in search results
        self.results_per_page = 5  # Number of results per page
        self.load_more_timestamp = None  # When the Load More button was added
        self.load_more_message_id = None  # ID of message with Load More button
        self.last_search_time = None  # When the last search was performed
        self.timeout_job = None  # Job for timeout handling

user_states = {}  # Dictionary to track UserState objects for each user

# Timeout settings
LOAD_MORE_TIMEOUT = 300  # 5 minutes in seconds

def search_arxiv(query: str, max_results=5):
    try:
        logger.info(f"Searching arXiv using arxiv package for: {query}")
        
        # Create a customized session with proper timeout and retry settings
        session = requests.Session()
        
        # Optional: Configure proxy if needed
        # proxy_url = "http://your-proxy-server:port"
        # session.proxies = {
        #     "http": proxy_url,
        #     "https": proxy_url
        # }
        
        # Configure retry strategy with more conservative exponential backoff
        retry_strategy = Retry(
            total=2,  # Increased maximum number of retries
            backoff_factor=2,  # Increased exponential backoff factor
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
            allowed_methods=["GET"],  # Only retry on GET requests
            respect_retry_after_header=True,  # Respect Retry-After headers
            connect=4,  # More retries on connection errors
            read=4,     # More retries on read errors
        )
        
        # Configure adapters for both HTTP and HTTPS
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=5,  # Reduced connection pool
            pool_maxsize=5      # Reduced max pool size to conserve resources
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set more generous timeouts for connect and read operations
        session.timeout = (15, 90)  # (connect timeout, read timeout) in seconds
        
        logger.info("Configured session with connect timeout=15s, read timeout=90s")
        
        # Create a search object with appropriate parameters
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance
        )
        
        # Create a client with more conservative settings
        client = arxiv.Client(
            page_size=10,  # Reduced page size to minimize data transfer
            delay_seconds=5,  # Increased delay between requests
            num_retries=5    # Increased number of retries
        )
        
        # Replace the default session with our custom one
        client._session = session
        
        logger.info("Configured arxiv client with page_size=10, delay=5s, retries=5")
        
        entries = []
        
        # Use the client to get results with proper timeout handling
        try:
            # Execute the search and process results
            for result in client.results(search):
                try:
                    title = result.title.strip()
                    link = result.entry_id  # This is the URL to the paper
                    
                    # Process the summary, limiting to 500 chars
                    if result.summary:
                        summary = result.summary.strip()[:500] + "..."
                    else:
                        summary = "No summary available"
                    
                    entries.append({
                        "title": title, 
                        "link": link,
                        "summary": summary
                    })
                except Exception as e:
                    logger.error(f"Error processing entry: {e}")
                    continue
        except requests.exceptions.ConnectTimeout as e:
            logger.error(f"Connection timeout error in arxiv client: {e}")
            return {
                "error": "connect_timeout",
                "message": "Could not connect to arXiv server. Please try again later.",
            }
        except requests.exceptions.ReadTimeout as e:
            logger.error(f"Read timeout error in arxiv client: {e}")
            return {
                "error": "read_timeout",
                "message": "The arXiv server took too long to respond. Please try again later.",
            }
        except requests.exceptions.Timeout as e:
            logger.error(f"General timeout error in arxiv client: {e}")
            return {
                "error": "timeout",
                "message": "The request to arXiv timed out. Please try again later.",
            }
        except urllib3.exceptions.MaxRetryError as e:
            logger.error(f"Max retry error in arxiv client: {e}")
            return {
                "error": "max_retries",
                "message": "Maximum retry attempts exceeded when connecting to arXiv. The service might be experiencing issues.",
            }
        except urllib3.exceptions.ProtocolError as e:
            logger.error(f"Protocol error in arxiv client: {e}")
            return {
                "error": "protocol_error",
                "message": "A protocol error occurred when connecting to arXiv. Please try again later.",
            }
                
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
        if user_id not in user_states:
            user_states[user_id] = UserState()
        user_states[user_id].state = "awaiting_query"
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
        if user_id not in user_states:
            user_states[user_id] = UserState()
        user_states[user_id].state = "awaiting_query"
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

        # Initialize user state if not exists
        if user_id not in user_states:
            user_states[user_id] = UserState()

        # Cancel any existing load more timeout job
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
            
            # Perform complete cleanup of any Load More state
            await cleanup_load_more_state(user_id, context)

            # Send a searching message to provide immediate feedback (with keyboard removed)
            processing_message = await update.message.reply_text(
                "🔍 Searching for papers... Please wait.",
                reply_markup=ReplyKeyboardRemove(),
            )

            await send_paper_results(update, context, query, processing_message)
        else:
            # If user just sent text without being in search mode, treat it as a search
            query = message_text
            
            # Clean up any existing Load More state
            await cleanup_load_more_state(user_id, context)
            
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


async def update_progress_bar(update, context, processing_message):
    try:
        for progress in range(0, 101, 10):  # Increment progress by 10%
            await asyncio.sleep(1)  # Simulate progress every second
            try:
                if processing_message:
                    await processing_message.edit_text(
                        text=f"🔄 Searching... {progress}% complete"
                    )
            except telegram.error.BadRequest as e:
                logger.warning(f"Failed to update progress: {e}")
                break  # Stop updating if the message is deleted or edited elsewhere
    except asyncio.CancelledError:
        pass  # Handle cancellation gracefully

async def cleanup_load_more_state(user_id, context):
    """Clean up any existing Load More state for a user"""
    try:
        if user_id in user_states:
            user_state = user_states[user_id]
            
            # Cancel any existing timeout job
            if user_state.timeout_job:
                try:
                    user_state.timeout_job.schedule_removal()
                except Exception as e:
                    logger.warning(f"Error removing scheduled job: {e}")
                user_state.timeout_job = None
            
            # Reset load more timestamp and message ID
            user_state.load_more_timestamp = None
            user_state.load_more_message_id = None
            
            logger.debug(f"Cleaned up Load More state for user {user_id}")
    except Exception as e:
        logger.error(f"Error during cleanup of Load More state: {e}")
        user_state.load_more_message_id = None


async def send_load_more_timeout_message(context: ContextTypes.DEFAULT_TYPE):
    """Send a reminder message after timeout for Load More button"""
    job = context.job
    user_id = job.data.get("user_id")
    chat_id = job.data.get("chat_id")
    
    if user_id in user_states:
        user_state = user_states[user_id]
        
        # Only send the timeout message if the load more timestamp hasn't changed
        if (user_state.load_more_timestamp and 
            (datetime.now() - user_state.load_more_timestamp).total_seconds() >= LOAD_MORE_TIMEOUT):
            
            try:
                if chat_id:
                    # Send informative and actionable timeout message
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="⏱️ It's been a while since you checked the \"Load More\" results for your search. You can either click the \"Load More\" button to continue viewing results, or start a new search using the 🔍 Search button.",
                        reply_markup=get_main_keyboard()
                    )
                    
                    # Clear all load more state for this user
                    user_state.timeout_job = None
                    user_state.load_more_timestamp = None
                    user_state.load_more_message_id = None
                    
                    # We don't actually invalidate the query to allow the user
                    # to restart the same search if they want
            except Exception as e:
                logger.error(f"Error sending timeout message: {e}")


async def handle_load_more(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the Load More button callback"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    
    # Check if this is the most recent Load More button and the button is still valid
    if (user_id not in user_states or 
        not user_states[user_id].query or 
        not user_states[user_id].load_more_message_id or
        user_states[user_id].load_more_message_id != message_id):
        await query.message.reply_text(
            "Your search session has expired. Please start a new search.",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Get the stored search query and increment page
    user_state = user_states[user_id]
    stored_query = user_state.query
    user_state.current_page += 1
    
    # Cancel any existing timeout job
    if user_state.timeout_job:
        user_state.timeout_job.schedule_removal()
        user_state.timeout_job = None
    
    # Set up a new Processing message
    processing_message = await query.message.reply_text(
        "🔄 Loading more results... Please wait."
    )
    
    # Use the same send_paper_results function with the next page
    await send_paper_results(
        update, 
        context, 
        stored_query, 
        processing_message,
        is_load_more=True
    )


async def send_paper_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    processing_message=None,
    is_load_more=False,
):
    try:
        user_id = update.effective_user.id
        
        # Initialize user state if needed
        if user_id not in user_states:
            user_states[user_id] = UserState()
        
        user_state = user_states[user_id]
        
        # If this is a new search (not load more), reset the page counter
        if not is_load_more:
            user_state.current_page = 0
            user_state.query = query
            
            # Clean up any existing Load More state for new searches
            await cleanup_load_more_state(user_id, context)
        
        page = user_state.current_page
        results_per_page = user_state.results_per_page
        
        logger.info(f"Searching arXiv for: {query} (page {page+1})")

        # Start the progress bar task
        progress_task = asyncio.create_task(
            update_progress_bar(update, context, processing_message)
        )

        # Calculate the start index for pagination
        max_results = results_per_page if page == 0 else results_per_page * (page + 1)

        # Perform the search with pagination
        result = search_arxiv(query, max_results=max_results)

        # Cancel the progress bar task if it hasn't completed
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
            await update.effective_message.reply_text(
                "No papers found. Please try a different search term.",
                reply_markup=get_main_keyboard(),
            )
            return

        # If this is a load more request, only show new papers
        if is_load_more and page > 0:
            # Calculate the starting index for the new batch
            start_index = results_per_page * page
            # If we've loaded all results already, don't show anything new
            if start_index >= len(papers):
                await update.effective_message.reply_text(
                    "No more papers available for this search.",
                    reply_markup=get_main_keyboard(),
                )
                return
            # Get only the new papers
            papers_to_show = papers[start_index:start_index + results_per_page]
        else:
            # For new searches, just show the first batch
            papers_to_show = papers[:results_per_page]

        if not is_load_more:
            logger.info(f"Found {len(papers)} papers for query: {query}")
            await update.effective_message.reply_text(
                f"📚 Found {len(papers)} papers matching your search."
            )
        else:
            logger.info(f"Loading more results for query: {query} (page {page+1})")

        for i, paper in enumerate(papers_to_show):
            try:
                msg = f"📄 *{paper['title']}*\n\n{paper['summary']}\n\n🔗 [Read more]({paper['link']})"

                # For the last paper in this batch
                if i == len(papers_to_show) - 1:
                    # Check if there are more papers to show
                    has_more = False
                    if is_load_more:
                        # Check if there are more papers to load after this batch
                        next_index = results_per_page * (page + 1)
                        has_more = next_index < len(papers)
                    else:
                        # For new searches, check if there are more results available
                        has_more = len(papers) > results_per_page
                    
                    # If we have more papers to show, add the "Load More" button
                    if has_more:
                        # Create inline keyboard with Load More button
                        keyboard = [[InlineKeyboardButton("📚 Load More Results", callback_data="load_more")]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        # Send the last paper with the Load More button
                        last_message = await update.effective_message.reply_markdown(
                            msg, reply_markup=reply_markup
                        )
                        
                        # Store the timestamp and message ID for timeout handling
                        user_state.load_more_timestamp = datetime.now()
                        user_state.load_more_message_id = last_message.message_id
                        
                        # Cancel any existing timeout job
                        if user_state.timeout_job:
                            user_state.timeout_job.schedule_removal()
                        
                        # Schedule a new timeout job to remind user after inactivity
                        chat_id = update.effective_chat.id
                        job_data = {"user_id": user_id, "chat_id": chat_id}
                        
                        user_state.timeout_job = context.job_queue.run_once(
                            send_load_more_timeout_message,
                            LOAD_MORE_TIMEOUT,
                            data=job_data
                        )
                        logger.info(f"Scheduled load_more timeout for user {user_id} in {LOAD_MORE_TIMEOUT} seconds")
                    else:
                        # No more results to show, just send with main keyboard
                        await update.effective_message.reply_markdown(
                            msg, reply_markup=get_main_keyboard()
                        )
                else:
                    # For non-last papers in the batch, send without any buttons
                    await update.effective_message.reply_markdown(msg)
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
    app.add_handler(CallbackQueryHandler(handle_load_more, pattern="^load_more$"))
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
