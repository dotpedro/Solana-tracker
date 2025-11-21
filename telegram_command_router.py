import os
import re
import traceback
import asyncio
import logging
import json
from datetime import date
from dotenv import load_dotenv

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

import pytz


def main() -> None:
    """Starts the Telegram bot application."""
    logger.info("Starting Telegram Bot Application from Command Router...")

    application = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    application.post_init = set_bot_commands
    register_handlers(application)

    logger.info("Bot is initialized. Starting polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Bot polling stopped.")



import glob # Added for /recheck_mint
from datetime import date 
from dotenv import load_dotenv

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

# --- Load Environment Variables ---
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    print("[!] CRITICAL: Telegram Bot Token (TELEGRAM_BOT_TOKEN) not found in environment variables. Exiting.")
    exit()
# --- End Environment Variables ---

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING) 
logger = logging.getLogger(__name__)

# --- Eco Mode Import ---
try:
    from eco_mode_config import set_eco_mode, is_eco_mode
    logger.info("[✓] Eco Mode configuration loaded.")
except ImportError:
    logger.warning("[!] Warning: eco_mode_config.py not found. Eco mode commands disabled.")
    def set_eco_mode(enable: bool): logger.warning("Eco mode config not available."); return False
    def is_eco_mode(): logger.warning("Eco mode config not available."); return False
# --- End Eco Mode Import ---

# --- Project-Specific Imports ---
try:
    from telegram_alert_router import escape_markdown_v2 as router_escape_markdown_v2
    logger.info("[✓] Markdown escape utility loaded from telegram_alert_router.")
except ImportError:
    logger.critical("[!] CRITICAL IMPORT ERROR in telegram_command_router.py: telegram_alert_router.escape_markdown_v2 not found.")
    def router_escape_markdown_v2(text: str) -> str: return text

try:
    from telegram_manual_checker import (
        perform_safety_audit,
        add_token_to_tracking,
        remove_token_from_tracking,
        remove_all_tokens_for_chat,
        load_tracked_tokens
    )
    logger.info("[✓] Successfully imported core logic from telegram_manual_checker.")
except ImportError as e:
    logger.critical(f"[!] CRITICAL IMPORT ERROR: Could not import from telegram_manual_checker: {e}")
    async def perform_safety_audit(ca_param): return f"Error: Core logic unavailable due to import error: {e}", "N/A", "N/A"
    def add_token_to_tracking(ca_param, chat_id_param, symbol_param, name_param): return False
    def remove_token_from_tracking(ca_param, chat_id_param): return False
    def remove_all_tokens_for_chat(chat_id_param): return 0
    def load_tracked_tokens(): return []
    

try:
    import telegram_alert_router # <--- ENSURE THIS IS PRESENT AND CORRECT
    logger.info("[✓] Successfully imported telegram_alert_router module.")
except ImportError:
    logger.error("[!] Failed to import telegram_alert_router. Alert sending will fail.")
    telegram_alert_router = None # type: ignore
    
# --- End Project-Specific Imports ---

# --- Define PENDING_REPORTS_DIR for /recheck_mint command ---
REPORTS_BASE_DIR_FOR_TELEGRAM = "reports" 
PENDING_REPORTS_DIR_FOR_TELEGRAM = os.path.join(REPORTS_BASE_DIR_FOR_TELEGRAM, "holdings", "pending")
# --- End PENDING_REPORTS_DIR Definition ---


async def set_bot_commands(application: Application) -> None:
    """Sets the list of commands available in the bot menu during startup."""
    commands = [
        BotCommand("start", "Welcome & Help"),
        BotCommand("check", "<CA> Audit a token & option to track"),
        BotCommand("my_tracked", "List your tracked tokens"),
        BotCommand("untrack", "<CA> Stop tracking a token"),
        BotCommand("untrack_all", "Stop tracking all your tokens"),
        BotCommand("api_usage", "📊 Show API credit usage today"), 
        BotCommand("recheck_mint", "<MINT> Force reprocess reports with this mint"),
        BotCommand("eco_on", "🌿 Activate Eco Mode (Lower API usage)"),
        BotCommand("eco_off", "🚀 Deactivate Eco Mode (Full performance)"),
        BotCommand("eco_status", "📊 Check current Eco Mode status")
    ]
    try:
        await application.bot.set_my_commands(commands)
        logger.info("[✓] Bot commands updated successfully via post_init.")
    except Exception as e:
        logger.error(f"[!] Failed to set bot commands during startup: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command."""
    welcome_text = (
        "👋 Welcome to the Token Safety Checker Bot\\!\n\n"
        "*Main Commands:*\n"
        "`/check <Solana CA>` \\- Get a safety audit & option to track\\.\n"
        "`/my_tracked` \\- List tokens you are currently tracking\\.\n"
        "`/untrack <Solana CA>` \\- Stop tracking a specific token\\.\n"
        "`/untrack_all` \\- Stop tracking all tokens for your chat\\.\n\n"
        "*Utility Commands:*\n" 
        "`/api_usage` \\- 📊 Show current API credit usage for today\\.\n"
        "`/recheck_mint <MINT>` \\- 🔁 Force re\\-processing of pending reports containing this mint address\\. This is useful in Eco Mode if a report was skipped\\.\n\n"
        "*Eco Mode Commands:*\n"
        "`/eco_on` \\- 🌿 Activate lower API usage mode\\.\n"
        "`/eco_off` \\- 🚀 Deactivate Eco Mode for full performance\\.\n"
        "`/eco_status` \\- 📊 Check if Eco Mode is ON or OFF\\.\n\n"
        "The `token_tracker_service\\.py` script \\(if running separately\\) "
        "will monitor tracked tokens for buy/sell signals\\.\n\n"
        "*Note:* Eco Mode changes affect background scripts like `main_loop_analyzer.py` or `report_processor.py` "
        "on their *next cycle* or if they are *restarted*\\."
    )
    await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN_V2)
    try:
        await set_bot_commands(context.application)
    except Exception as e:
        logger.warning(f"Failed to set commands during /start (potential redundancy): {e}")


async def _process_ca_and_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, contract_address: str) -> None:
    """Internal helper to analyze a CA and send results + tracking buttons."""
    chat_id = update.effective_chat.id
    escaped_ca_display = router_escape_markdown_v2(contract_address)

    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", contract_address):
        await update.message.reply_text(f"⚠️ `{escaped_ca_display}` invalid Solana address format\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    thinking_message = await update.message.reply_text(f"🔍 Analyzing `{escaped_ca_display}`\\.\\.\\. Please wait\\.", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        report, raw_symbol, raw_name = await perform_safety_audit(contract_address)
        context_key = f"check_context_{contract_address}"
        context.chat_data[context_key] = {"symbol": raw_symbol, "name": raw_name}
        logger.debug(f"[Debug] Stored context for {contract_address}: Symbol='{raw_symbol}', Name='{raw_name}' under key {context_key}")

        max_len = 4050 
        parts = []
        if len(report) > max_len:
            current_part = ""
            lines = report.split('\n')
            for line_content in lines:
                if len(current_part) + len(line_content) + 1 > max_len:
                    if current_part: parts.append(current_part)
                    current_part = line_content
                else:
                    current_part = (current_part + "\n" + line_content) if current_part else line_content
            if current_part: parts.append(current_part)
        else:
            parts.append(report)

        if parts:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=thinking_message.message_id, text=parts[0], parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            for part_content in parts[1:]:
                await update.message.reply_text(text=part_content, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

        if not report.startswith("❌"):
            keyboard = [[InlineKeyboardButton("✅ Yes, track it!", callback_data=f"track_yes_{contract_address}"),
                         InlineKeyboardButton("❌ No, thanks.", callback_data=f"track_no_{contract_address}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"Track *{router_escape_markdown_v2(raw_symbol or contract_address[:6])}* for signals?", 
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
    except Exception as e:
        error_message_text = router_escape_markdown_v2(f"An error occurred: {str(e)[:200]}")
        error_text = f"🚨 Error processing `{escaped_ca_display}`\\: {error_message_text}"
        logger.error(f"Error in _process_ca_and_reply for {contract_address}: {e}", exc_info=True) 
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=thinking_message.message_id, text=error_text, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as edit_err:
            logger.warning(f"Could not edit 'thinking' message to show error (maybe too old?): {edit_err}")
            await update.message.reply_text(error_text, parse_mode=ParseMode.MARKDOWN_V2)


async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /check command."""
    if not context.args:
        await update.message.reply_text("Usage: `/check <Solana CA>`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    ca_to_check = context.args[0].strip()
    await _process_ca_and_reply(update, context, ca_to_check)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles plain text messages to potentially extract and check a CA."""
    if update.message and update.message.text:
        potential_cas = re.findall(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b", update.message.text)
        if potential_cas:
            ca_found = potential_cas[0].strip()
            logger.info(f"Detected potential CA in message: {ca_found}")
            await _process_ca_and_reply(update, context, ca_found)


async def handle_tracking_decision_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses for tracking decisions (Yes/No)."""
    query = update.callback_query
    await query.answer()

    try:
        action, decision, ca_from_button = query.data.split('_', 2)
        if action != "track" or decision not in ["yes", "no"] or not ca_from_button:
            raise ValueError("Invalid callback format")
        logger.info(f"[Callback] Received: Action='{action}', Decision='{decision}', CA='{ca_from_button}'")
    except ValueError:
        try: await query.edit_message_text(text="⚠️ Internal error: Invalid callback data format\\.", parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e: logger.error(f"[!] Error editing message on invalid callback format: {e}")
        logger.error(f"[!] Invalid callback data received: {query.data}"); return

    chat_id_for_tracking = query.message.chat.id if query.message else None
    if not chat_id_for_tracking:
        try: await query.edit_message_text(text="⚠️ Could not determine chat ID for tracking\\. Cannot save decision\\.", parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e: logger.error(f"[!] Error editing message on missing chat_id: {e}")
        logger.error(f"[!] Could not get chat_id from callback for CA {ca_from_button}"); return

    context_key = f"check_context_{ca_from_button}"
    check_data = context.chat_data.pop(context_key, None)

    if check_data:
        current_symbol_for_storage = check_data.get("symbol", "N/A")
        current_name_for_storage = check_data.get("name", "N/A")
        logger.debug(f"[Callback] Retrieved context for {ca_from_button}: Symbol='{current_symbol_for_storage}', Name='{current_name_for_storage}'")
    else:
        current_symbol_for_storage = "N/A"; current_name_for_storage = "N/A"
        logger.warning(f"[Callback] Context not found for {ca_from_button} (key: {context_key}). Using defaults.")

    current_symbol_display = router_escape_markdown_v2(current_symbol_for_storage if current_symbol_for_storage != 'N/A' else ca_from_button[:6])
    escaped_ca_md = router_escape_markdown_v2(ca_from_button)

    message = ""
    if decision == "yes":
        if add_token_to_tracking(ca_from_button, chat_id_for_tracking, current_symbol_for_storage, current_name_for_storage):
            message = f"✅ Now tracking *{current_symbol_display}* \\(`{escaped_ca_md}`\\) for signals\\.\n" \
                      f"The `token_tracker_service\\.py` script \\(if running\\) will send alerts\\."
            logger.info(f"[Tracking] Added {ca_from_button} for chat {chat_id_for_tracking}")
        else:
            message = f"ℹ️ *{current_symbol_display}* \\(`{escaped_ca_md}`\\) is already being tracked for this chat\\."
            logger.info(f"[Tracking] Already tracking {ca_from_button} for chat {chat_id_for_tracking}")
    elif decision == "no":
        message = f"Okay, not tracking *{current_symbol_display}* \\(`{escaped_ca_md}`\\)\\."
        logger.info(f"[Tracking] User chose not to track {ca_from_button} for chat {chat_id_for_tracking}")

    try:
        await query.edit_message_text(text=message, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.warning(f"[!] Error editing message text in callback handler ({type(e).__name__}: {e}). Message might be too old or already changed.")


async def my_tracked_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /my_tracked command."""
    chat_id = str(update.effective_chat.id)
    all_tracked = load_tracked_tokens()
    user_tracked = [t for t in all_tracked if str(t.get("chat_id")) == chat_id]

    if not user_tracked:
        await update.message.reply_text("You are not currently tracking any tokens\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    message_lines = ["*Tokens you are currently tracking:*\n"]
    user_tracked.sort(key=lambda x: x.get("symbol", "zzz") if x.get("symbol", "N/A") != "N/A" else "zzz" + x.get("ca", ""))
    for token_info in user_tracked:
        symbol = token_info.get("symbol", "N/A")
        ca = token_info.get("ca", "UNKNOWN_CA")
        name = token_info.get("name", "")
        display_symbol = router_escape_markdown_v2(symbol if symbol != "N/A" else ca[:6])
        display_ca = router_escape_markdown_v2(ca)
        display_name_part = f" \\({router_escape_markdown_v2(name)}\\)" if name and name != "N/A" else ""
        message_lines.append(f"\\- *{display_symbol}*{display_name_part}\n  `{display_ca}`")

    full_message = "\n".join(message_lines)
    max_len = 4050
    if len(full_message) > max_len:
        await update.message.reply_text(
            f"You are tracking {len(user_tracked)} tokens\\. The list is too long to display fully here\\. "
            f"Please use `/untrack <CA>` or `/untrack_all` to manage them\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    else:
        await update.message.reply_text(full_message, parse_mode=ParseMode.MARKDOWN_V2)


async def untrack_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /untrack command."""
    chat_id = str(update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("Usage: `/untrack <CA_TO_UNTRACK>`", parse_mode=ParseMode.MARKDOWN_V2)
        return

    ca_to_untrack = context.args[0].strip()
    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", ca_to_untrack):
        await update.message.reply_text(f"⚠️ Invalid CA format: `{router_escape_markdown_v2(ca_to_untrack)}`", parse_mode=ParseMode.MARKDOWN_V2)
        return

    if remove_token_from_tracking(ca_to_untrack, chat_id):
        await update.message.reply_text(f"✅ Stopped tracking token: `{router_escape_markdown_v2(ca_to_untrack)}`", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"[Tracking] Removed {ca_to_untrack} for chat {chat_id}")
    else:
        await update.message.reply_text(f"ℹ️ You were not tracking token: `{router_escape_markdown_v2(ca_to_untrack)}`", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"[Tracking] Attempted to remove {ca_to_untrack} for chat {chat_id}, but it wasn't tracked.")


async def untrack_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /untrack_all command."""
    chat_id = str(update.effective_chat.id)
    removed_count = remove_all_tokens_for_chat(chat_id)
    if removed_count > 0:
        await update.message.reply_text(f"✅ Stopped tracking all {removed_count} token\\(s\\) for this chat\\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"[Tracking] Removed all {removed_count} tokens for chat {chat_id}")
    else:
        await update.message.reply_text("ℹ️ You were not tracking any tokens in this chat\\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"[Tracking] Attempted to remove all tokens for chat {chat_id}, but none were tracked.")




# --- API Usage Command Handler ---
async def api_usage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /api_usage command to show current API usage stats."""
    logger.info("[Command] /api_usage received.")
    usage_file_path = os.getenv("API_USAGE_FILE", "api_daily_usage.json")
    
    try:
        with open(usage_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        date_str = data.get("date", str(date.today()))
        helius_usage = data.get("helius_credits_today", 0)
        birdeye_usage = data.get("birdeye_credits_today", 0)
        gmgn_usage = data.get("gmgn_calls_today", 0)
        other_usage = data.get("other_calls_today", 0)

        helius_cap = int(os.getenv("HELIUS_DAILY_CAP", "322580"))
        birdeye_cap = int(os.getenv("BIRDEYE_DAILY_CAP", "322580"))

        escaped_date_str = router_escape_markdown_v2(date_str)

        message = (
            f"📊 *API Usage Today* \\({escaped_date_str}\\):\n\n"
            f"🔵 Helius: `{helius_usage:,}` / `{helius_cap:,}`\n"
            f"🟣 Birdeye: `{birdeye_usage:,}` / `{birdeye_cap:,}`\n"
            f"🟢 GMGN: `{gmgn_usage:,}`\n"
            f"🔘 Other: `{other_usage:,}`\n\n"
            f"Use `/eco_status` to check Eco Mode status\\."
        )
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN_V2)

    except FileNotFoundError:
        logger.warning(f"API usage file not found: {usage_file_path}")
        escaped_filename = router_escape_markdown_v2(os.path.basename(usage_file_path))
        await update.message.reply_text(
            f"⚠️ API usage data file \\(`{escaped_filename}`\\) not found\\. Cannot display stats\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from API usage file: {usage_file_path}", exc_info=True)
        escaped_filename = router_escape_markdown_v2(os.path.basename(usage_file_path))
        await update.message.reply_text(
            f"⚠️ Error reading API usage data from file \\(`{escaped_filename}`\\)\\. File might be corrupted\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"[!] /api_usage command error: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ An unexpected error occurred while fetching API usage stats\\. Please check server logs\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
# --- End API Usage Command Handler ---

# --- New Command Handler for /recheck_mint ---
async def recheck_mint_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Flags pending reports containing a specific mint for forced re-processing."""
    if not context.args:
        await update.message.reply_text(
            "Usage: `/recheck_mint <MINT_ADDRESS>`\n"
            "Please provide the Solana token mint address you want to force re\\-check in pending reports\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    mint_to_recheck = context.args[0].strip()
    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", mint_to_recheck):
        await update.message.reply_text(
            f"⚠️ Invalid Solana mint address format: `{router_escape_markdown_v2(mint_to_recheck)}`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    logger.info(f"[Command /recheck_mint] Received for mint: {mint_to_recheck}")
    
    if not os.path.isdir(PENDING_REPORTS_DIR_FOR_TELEGRAM):
        logger.error(f"/recheck_mint: PENDING_REPORTS_DIR_FOR_TELEGRAM ('{PENDING_REPORTS_DIR_FOR_TELEGRAM}') does not exist or is not a directory.")
        await update.message.reply_text(
            f"⚠️ Server Error: The directory for pending reports (`{router_escape_markdown_v2(os.path.basename(PENDING_REPORTS_DIR_FOR_TELEGRAM))}`) was not found\\. Cannot proceed\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    thinking_message = await update.message.reply_text(
        f"Searching for pending reports containing mint `{router_escape_markdown_v2(mint_to_recheck)}` and attempting to flag them for re\\-check\\.\\.\\.",
        parse_mode=ParseMode.MARKDOWN_V2
    )

    found_and_flagged_count = 0
    already_flagged_count = 0
    error_files = []
    files_checked = 0

    pending_files_pattern = os.path.join(PENDING_REPORTS_DIR_FOR_TELEGRAM, "*.json")
    for report_filepath in glob.glob(pending_files_pattern):
        files_checked += 1
        try:
            with open(report_filepath, "r+", encoding="utf-8") as f: 
                report_content = json.load(f)
                mint_found_in_report = False
                
                correlated_holdings = report_content.get("correlated_holdings", [])
                if isinstance(correlated_holdings, list):
                    for item in correlated_holdings:
                        if isinstance(item, dict) and item.get("token_mint") == mint_to_recheck:
                            mint_found_in_report = True
                            break
                
                if not mint_found_in_report:
                    snapshot_holdings = report_content.get("correlated_holdings_snapshot", [])
                    if isinstance(snapshot_holdings, list):
                        for item in snapshot_holdings:
                            if isinstance(item, dict) and item.get("token_mint") == mint_to_recheck:
                                mint_found_in_report = True
                                break
                
                if mint_found_in_report:
                    if report_content.get("force_recheck") is True:
                        logger.info(f"Mint {mint_to_recheck} found in {os.path.basename(report_filepath)}, already flagged for recheck.")
                        already_flagged_count +=1
                    else:
                        report_content["force_recheck"] = True
                        f.seek(0)  
                        json.dump(report_content, f, indent=2)
                        f.truncate() 
                        logger.info(f"Mint {mint_to_recheck} found and flagged for recheck in {os.path.basename(report_filepath)}")
                        found_and_flagged_count += 1
        except json.JSONDecodeError:
            logger.warning(f"/recheck_mint: Invalid JSON in {report_filepath}. Skipping.")
            error_files.append(os.path.basename(report_filepath) + " \\(invalid JSON\\)")
        except IOError as e:
            logger.error(f"/recheck_mint: IOError for {report_filepath}: {e}")
            error_files.append(os.path.basename(report_filepath) + f" \\(read/write error: {router_escape_markdown_v2(str(e))}\\)")
        except Exception as e:
            logger.error(f"/recheck_mint: Unexpected error processing {report_filepath}: {e}", exc_info=True)
            error_files.append(os.path.basename(report_filepath) + " \\(unexpected error\\)")

    final_message_parts = []
    if found_and_flagged_count > 0:
        final_message_parts.append(f"✅ Successfully flagged {found_and_flagged_count} new pending report(s) containing mint `{router_escape_markdown_v2(mint_to_recheck)}` for forced re\\-processing\\.")
    if already_flagged_count > 0:
        final_message_parts.append(f"ℹ️ Found {already_flagged_count} report(s) already flagged for re\\-check containing the mint\\.")
    
    if not found_and_flagged_count and not already_flagged_count:
         final_message_parts.append(f"🤷 No pending reports currently found containing mint `{router_escape_markdown_v2(mint_to_recheck)}` that weren't already flagged\\. Checked {files_checked} files\\.")
    
    if error_files:
        errors_str = "\n\\- ".join(error_files) 
        final_message_parts.append(f"⚠️ Encountered errors with some files:\n\\- {errors_str}")

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=thinking_message.message_id,
        text="\n\n".join(final_message_parts),
        parse_mode=ParseMode.MARKDOWN_V2
    )
# --- End /recheck_mint Command Handler ---


# --- Eco Mode Command Handlers ---
async def eco_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activates Eco Mode."""
    logger.info("[Command] /eco_on received.")
    enable = True # For message formatting
    if set_eco_mode(True):
        # --- MODIFIED MESSAGE ---
        await update.message.reply_text(
            f"Eco mode is now {'ON' if enable else 'OFF'}\\. Background scripts will pick this up within ~1 cycle\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        # --- END MODIFICATION ---
    else:
        await update.message.reply_text("⚠️ Failed to activate Eco Mode\\. Check server logs\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def eco_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deactivates Eco Mode."""
    logger.info("[Command] /eco_off received.")
    enable = False # For message formatting
    if set_eco_mode(False):
        # --- MODIFIED MESSAGE ---
        await update.message.reply_text(
            f"Eco mode is now {'ON' if enable else 'OFF'}\\. Background scripts will pick this up within ~1 cycle\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        # --- END MODIFICATION ---
    else:
        await update.message.reply_text("⚠️ Failed to disable Eco Mode\\. Check server logs\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def eco_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Checks the current status of Eco Mode."""
    logger.info("[Command] /eco_status received.")
    status = is_eco_mode()
    mode_text = "*ON* 🌿 \\(Lower API Usage\\)" if status else "*OFF* 🚀 \\(Full Performance\\)"
    await update.message.reply_text(f"Eco Mode is currently {mode_text}\\.", parse_mode=ParseMode.MARKDOWN_V2)


def register_handlers(application: Application) -> None:
    """Registers all command, message, and callback handlers."""
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("check", check_command))
    application.add_handler(CommandHandler("my_tracked", my_tracked_command))
    application.add_handler(CommandHandler("untrack", untrack_command))
    application.add_handler(CommandHandler("untrack_all", untrack_all_command))
    application.add_handler(CommandHandler("api_usage", api_usage)) 
    application.add_handler(CommandHandler("recheck_mint", recheck_mint_command))
    application.add_handler(CommandHandler("eco_on", eco_on))
    application.add_handler(CommandHandler("eco_off", eco_off))
    application.add_handler(CommandHandler("eco_status", eco_status))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_tracking_decision_callback, pattern="^track_"))
    logger.info("[✓] All command and callback handlers registered.")



if __name__ == "__main__":
    main()

# --- END OF FILE telegram_command_router.py ---