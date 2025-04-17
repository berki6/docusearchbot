import os
import sys
import time
import signal
import logging
import asyncio
import requests
import arxiv
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
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


# --- Helper to fetch from arXiv ---
def search_arxiv(query: str, max_results=5):
    try:
        logger.info(f"Searching arXiv using arxiv package for: {query}")
        
        # Create a search client with appropriate parameters
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance
        )
        
        entries = []
        
        # Execute the search and process results
        for result in search.results():
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


async def send_paper_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    processing_message=None,
):
    try:
        logger.info(f"Searching arXiv for: {query}")

        # Start the progress bar task
        progress_task = asyncio.create_task(
            update_progress_bar(update, context, processing_message)
        )

        # Perform the search
        result = search_arxiv(query)

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
