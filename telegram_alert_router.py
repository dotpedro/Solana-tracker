# --- START OF FILE telegram_alert_router.py ---

"""
telegram_alert_router.py – Centralised Telegram alert/notification utilities
--------------------------------------------------------------------------
Handles sending messages via Telegram bots, consolidating logic for:
- Bot token selection based on category (env vars).
- Chat ID selection based on category (env vars).
- MarkdownV2 and legacy Markdown escaping.
- Automatic retry without Markdown on parsing errors.
- Message chunking for long messages.
- Optional prefixes for alerts vs. status updates.
- Synchronous and asynchronous sending interfaces.
- Retry logic for rate limits (429).
- Validation of Telegram API 'ok' status.
- Optional pre/post send callbacks for logging/metrics.
- Graceful shutdown of the async thread pool.

Environment Variables Used:
- TELEGRAM_BOT_TOKEN_DEFAULT: Fallback bot token.
- TELEGRAM_CHAT_ID_DEFAULT: Fallback chat ID.
- TELEGRAM_BOT_TOKEN_<CATEGORY_UPPERCASE>: Optional token for a specific category.
- TELEGRAM_CHAT_ID_<CATEGORY_UPPERCASE>: Optional chat ID for a specific category.

Version: 1.3.1 (Fixes NameError bug, addresses minor points)
"""

import os
import re
import json
import time
import logging
import asyncio
import atexit # For graceful shutdown
from typing import Any, Optional, Dict, Sequence, Union, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor

import requests
from dotenv import load_dotenv

# Type Alias for callbacks
AlertCallback = Callable[[Dict[str, Any], Optional[Dict[str, Any]]], None]

# Basic logging setup (Conditional configuration)
LOG = logging.getLogger("telegram_alert_router")
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

load_dotenv()

# Public API definition
__all__ = [
    "send_alert",
    "send_alert_async",
    "escape_markdown_v2",
    "escape_markdown_legacy",
    "AlertCallback"
]

# ---------------------------------------------------------------------------
# MARKDOWN HELPERS (Unchanged from v1.2)
# ---------------------------------------------------------------------------
_MD_V2_ESCAPE_CHARS = r"_*[]()~`>#+-=|{}.!"
_MD_LEGACY_ESCAPE_CHARS = r"_*`"


def escape_markdown_v2(text: Any) -> str:
    """Escape characters that Telegram MarkdownV2 interprets specially."""
    if not isinstance(text, str): text = str(text)
    text = text.replace("\\", "\\\\")
    for ch in _MD_V2_ESCAPE_CHARS: text = text.replace(ch, f"\\{ch}")
    return text


def escape_markdown_legacy(text: Any) -> str:
    """Escape characters for the older Telegram *Markdown* parse mode."""
    if not isinstance(text, str): text = str(text)
    text = text.replace("\\", "\\\\")
    for ch in _MD_LEGACY_ESCAPE_CHARS: text = text.replace(ch, f"\\{ch}")
    return text

# ---------------------------------------------------------------------------
# TOKEN / CHAT LOOK-UP (Unchanged from v1.2)
# ---------------------------------------------------------------------------
_DEFAULT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN_DEFAULT"
_DEFAULT_CHAT_ENV = "TELEGRAM_CHAT_ID_DEFAULT"
_FALLBACK_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
_FALLBACK_CHAT_ENV = "TELEGRAM_CHAT_ID"


def _get_env_fallback(key_list: Sequence[str]) -> Optional[str]:
    """Gets the first non-empty environment variable from a list."""
    for key in key_list:
        value = os.getenv(key)
        if value: return value
    return None


def _resolve_credential(
    cred_type: str, category: Optional[str], explicit: Optional[Union[str, int]]
) -> Optional[str]:
    """Resolves Token or Chat ID with stricter category checking."""
    if explicit: return str(explicit)
    default_env = _DEFAULT_TOKEN_ENV if cred_type == "Token" else _DEFAULT_CHAT_ENV
    fallback_env = _FALLBACK_TOKEN_ENV if cred_type == "Token" else _FALLBACK_CHAT_ENV
    cred_value = None
    if category:
        category_key_prefix = "TELEGRAM_BOT_TOKEN" if cred_type == "Token" else "TELEGRAM_CHAT_ID"
        category_key = f"{category_key_prefix}_{category.upper()}"
        cred_value = os.getenv(category_key)
        if cred_value:
            other_cred_type = "Chat ID" if cred_type == "Token" else "Token"
            other_category_key_prefix = "TELEGRAM_CHAT_ID" if other_cred_type == "Chat ID" else "TELEGRAM_BOT_TOKEN"
            other_category_key = f"{other_category_key_prefix}_{category.upper()}"
            if not os.getenv(other_category_key):
                LOG.error("Inconsistent category config for '%s': Found %s (%s) but missing %s (%s). Cannot proceed.",
                          category, cred_type, category_key, other_cred_type, other_category_key)
                return None
            return cred_value
    cred_value = _get_env_fallback([default_env, fallback_env])
    if not cred_value:
         LOG.error("No Telegram %s found for category '%s' or defaults (%s, %s)",
                  cred_type, category, default_env, fallback_env)
    return cred_value


def _token_for(category: Optional[str] = None, explicit: Optional[str] = None) -> Optional[str]:
    """Find the appropriate Telegram Bot Token."""
    return _resolve_credential("Token", category, explicit)


def _chat_for(category: Optional[str] = None, explicit: Optional[Union[str, int]] = None) -> Optional[str]:
    """Find the appropriate Telegram Chat ID."""
    return _resolve_credential("Chat ID", category, explicit)

# ---------------------------------------------------------------------------
# CORE SEND ROUTINE (Unchanged from v1.3)
# ---------------------------------------------------------------------------
_MAX_MESSAGE_LEN = 4096
_RETRY_DELAY_SECONDS = 2
_MAX_RETRIES = 1


def _chunk_message(text: str, limit: int = _MAX_MESSAGE_LEN) -> Sequence[str]:
    """Split *text* on paragraph boundaries so each part ≤ Telegram limit."""
    if len(text) <= limit: return [text]
    chunks: list[str] = []
    paragraphs = re.split(r"(\n\s*\n)", text)
    current_chunk = ""
    reconstructed_parts = []
    temp_part = ""
    # Use p_idx to avoid shadowing outer loop variables later (Suggestion ⚠️)
    for p_idx, part in enumerate(paragraphs):
        if p_idx % 2 == 0: temp_part += part
        else: temp_part += part; reconstructed_parts.append(temp_part); temp_part = ""
    if temp_part: reconstructed_parts.append(temp_part)
    for part in reconstructed_parts:
        part_stripped = part.strip()
        if not part_stripped: continue
        if len(current_chunk) + len(part) <= limit: current_chunk += part
        else:
            if current_chunk: chunks.append(current_chunk.strip())
            if len(part_stripped) > limit:
                LOG.warning("Forcefully splitting a long paragraph/line for Telegram.")
                for i in range(0, len(part_stripped), limit): chunks.append(part_stripped[i : i + limit])
                current_chunk = ""
            else: current_chunk = part_stripped
    if current_chunk: chunks.append(current_chunk.strip())
    return [c for c in chunks if c]


def _post(
    payload: Dict[str, Any],
    token: str,
    original_unescaped_text: str,
    before_send_callback: Optional[AlertCallback] = None,
    after_send_callback: Optional[AlertCallback] = None,
) -> bool:
    """Internal function to POST payload to Telegram with retry logic and callbacks."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    safe_url = url.replace(token, f"***{token[-6:]}")
    retries = 0
    last_exception: Optional[Exception] = None
    last_status_code: Optional[int] = None
    last_response_json: Optional[Dict[str, Any]] = None

    while retries <= _MAX_RETRIES:
        current_payload = payload.copy()
        parse_mode_used = current_payload.get("parse_mode")
        log_text_snippet = str(current_payload.get("text", ""))[:200].replace('\n', '\\n')

        if before_send_callback:
            try: before_send_callback(current_payload, None)
            except Exception as cb_err: LOG.error("before_send_callback failed: %s", cb_err)

        try:
            LOG.debug("Attempt %d: Telegram POST to %s (ParseMode: %s): %.100s...", retries + 1, safe_url, parse_mode_used, log_text_snippet)
            r = requests.post(url, data=current_payload, timeout=20)
            last_status_code = r.status_code
            r.raise_for_status()
            last_response_json = r.json()

            if last_response_json.get("ok"):
                LOG.debug("Telegram POST successful and response['ok'] is True.")
                if after_send_callback:
                    try: after_send_callback(current_payload, last_response_json)
                    except Exception as cb_err: LOG.error("after_send_callback (success) failed: %s", cb_err)
                return True
            else:
                 error_desc = last_response_json.get('description', 'Unknown error')
                 error_code = last_response_json.get('error_code', 'N/A')
                 LOG.error("Telegram POST HTTP 200, but response['ok'] is False. Code: %s, Desc: %s. Payload: %.80s...",
                          error_code, error_desc, log_text_snippet)
                 if after_send_callback:
                    try: after_send_callback(current_payload, last_response_json)
                    except Exception as cb_err: LOG.error("after_send_callback (api error) failed: %s", cb_err)
                 return False

        except requests.exceptions.HTTPError as http_err:
            last_exception = http_err
            status_code = http_err.response.status_code if http_err.response else 500
            last_status_code = status_code
            resp_text = http_err.response.text if http_err.response else "No Response Text"
            last_response_json = {"ok": False, "description": f"HTTP Error {status_code}", "error_code": status_code, "raw_response": resp_text[:500]}
            response_headers = http_err.response.headers if http_err.response else {}

            is_parse_error = (status_code == 400 and
                              ("can't parse entities" in resp_text.lower() or
                               "unsupported entities" in resp_text.lower() or
                               "can't parse markdown" in resp_text.lower() or
                               "parse_mode" in resp_text.lower()))

            if is_parse_error and parse_mode_used and retries < _MAX_RETRIES:
                LOG.warning("Telegram HTTPError 400 (Parse Error) for text: '%s...'. Retrying without parse_mode.", log_text_snippet)
                retries += 1
                payload.pop("parse_mode", None)
                payload["text"] = original_unescaped_text
                time.sleep(_RETRY_DELAY_SECONDS)
                continue

            elif status_code == 429 and retries < _MAX_RETRIES:
                retry_after = response_headers.get("Retry-After")
                sleep_time = _RETRY_DELAY_SECONDS
                if retry_after and retry_after.isdigit():
                    sleep_time = int(retry_after)
                    LOG.warning("Telegram HTTPError 429 (Rate Limit). Retrying after %d seconds.", sleep_time)
                else:
                     LOG.warning("Telegram HTTPError 429 (Rate Limit). Header 'Retry-After' missing or invalid ('%s'). Retrying after default %d seconds.", retry_after, sleep_time)
                retries += 1
                time.sleep(sleep_time + 0.1)
                continue

            else:
                 LOG.error("Telegram HTTPError %s. URL: %s, Payload: %.80s... - Response: %.200s", status_code, safe_url, log_text_snippet, resp_text)
                 if after_send_callback:
                    try: after_send_callback(current_payload, last_response_json)
                    except Exception as cb_err: LOG.error("after_send_callback (http error) failed: %s", cb_err)
                 return False

        except requests.exceptions.RequestException as req_err:
            last_exception = req_err
            last_response_json = {"ok": False, "description": f"Network Error: {req_err}", "error_code": "NETWORK"}
            LOG.error("Telegram RequestException: %s. URL: %s, Payload: %.80s...", req_err, safe_url, log_text_snippet)
            if after_send_callback:
               try: after_send_callback(current_payload, last_response_json)
               except Exception as cb_err: LOG.error("after_send_callback (network error) failed: %s", cb_err)
            return False
        except Exception as e:
            last_exception = e
            last_response_json = {"ok": False, "description": f"Unexpected Error: {e}", "error_code": "UNKNOWN"}
            LOG.exception("Unexpected error during Telegram POST. URL: %s", safe_url)
            if after_send_callback:
               try: after_send_callback(current_payload, last_response_json)
               except Exception as cb_err: LOG.error("after_send_callback (unknown error) failed: %s", cb_err)
            return False

    LOG.error("Failed to send message after %d retries.", _MAX_RETRIES)
    if after_send_callback:
        try: after_send_callback(payload, last_response_json or {"ok": False, "description": "Max retries reached", "error_code": last_status_code or "RETRY_FAIL"})
        except Exception as cb_err: LOG.error("after_send_callback (final failure) failed: %s", cb_err)
    return False


def send_alert(
    message: str,
    *,
    chat_id: Optional[Union[str, int]] = None,
    category: Optional[str] = None,
    prefix: Optional[str] = None,
    is_status_update: bool = False,
    escape_markdown: bool = True,
    parse_mode: str = "MarkdownV2",
    disable_web_page_preview: bool = True,
    bot_token_override: Optional[str] = None,
    before_send_callback: Optional[AlertCallback] = None,
    after_send_callback: Optional[AlertCallback] = None,
) -> bool:
    """
    Sends a message alert to the configured Telegram chat.
    Includes optional callbacks for pre/post send actions.
    (See docstring in v1.2 for other args)
    """
    token = _token_for(category, bot_token_override)
    chat_id_final = _chat_for(category, chat_id)

    if not token or not chat_id_final:
        print(f"\n--- CONSOLE ALERT ({category or 'default'}) ---")
        if prefix: print(prefix.strip())
        print(message)
        print("--- (Telegram not configured) ---\n")
        if after_send_callback:
            try: after_send_callback({"text": message}, {"ok": False, "description": "Telegram not configured", "error_code": "CONFIG"})
            except Exception as cb_err: LOG.error("after_send_callback (config error) failed: %s", cb_err)
        return True

    unescaped_prefix = ""
    if prefix is None:
        prefix_text = "ℹ️ Status Update ℹ️" if is_status_update else "🔔 Alert 🔔"
        if parse_mode == "Markdown": unescaped_prefix = prefix_text.replace("*", "").replace("_", "") + "\n\n"
        else: unescaped_prefix = prefix_text + "\n\n"
    elif prefix: unescaped_prefix = prefix + "\n\n"

    prefix_to_send = unescaped_prefix
    message_to_send = message
    if escape_markdown:
        escape_func = escape_markdown_v2 if parse_mode == "MarkdownV2" else (escape_markdown_legacy if parse_mode == "Markdown" else None)
        if escape_func:
            prefix_to_send = escape_func(unescaped_prefix)
            message_to_send = escape_func(message)
            LOG.debug("Escaped prefix and message body for %s.", parse_mode)

    full_text_escaped = f"{prefix_to_send}{message_to_send}"
    full_text_original = f"{unescaped_prefix}{message}"

    common_payload: Dict[str, Any] = {
        "chat_id": chat_id_final,
        "disable_web_page_preview": "true" if disable_web_page_preview else "false",
    }
    if parse_mode: common_payload["parse_mode"] = parse_mode

    success = True
    chunks = _chunk_message(full_text_escaped)
    # --- BUG FIX: Assign num_chunks *before* using it ---
    num_chunks = len(chunks)
    original_chunks = _chunk_message(full_text_original) if num_chunks > 1 else [full_text_original]
    # --- End Bug Fix ---

    if len(chunks) != len(original_chunks):
         LOG.warning("Escaped vs Original text chunked differently (%d vs %d). Plain text retry might format unexpectedly.",
                     len(chunks), len(original_chunks))
         original_chunks = [full_text_original] * num_chunks

    LOG.info("Sending message to category '%s' (chat: %s) in %d chunk(s).",
             category or 'default', chat_id_final, num_chunks)

    for i, chunk in enumerate(chunks):
        payload = common_payload.copy()
        payload["text"] = chunk
        original_chunk_for_retry = original_chunks[min(i, len(original_chunks)-1)]

        LOG.debug("Sending chunk %d/%d (length: %d)", i + 1, num_chunks, len(chunk))

        if not _post(payload, token, original_chunk_for_retry, before_send_callback, after_send_callback):
            LOG.error("Failed to send chunk %d/%d for category '%s'.", i + 1, num_chunks, category or 'default')
            success = False
            time.sleep(0.5)
        elif num_chunks > 1:
             time.sleep(0.5)

    if success: LOG.info("Message sent successfully to category '%s'.", category or 'default')
    else: LOG.error("One or more message chunks failed to send for category '%s'.", category or 'default')

    return success

# ---------------------------------------------------------------------------
# ASYNC BRIDGE (Lazy Executor Creation)
# ---------------------------------------------------------------------------

_telegram_executor: Optional[ThreadPoolExecutor] = None
_executor_lock = asyncio.Lock() # Lock remains global

async def send_alert_async(**kwargs: Any) -> bool:
    """
    Asynchronous wrapper for send_alert using a dedicated ThreadPoolExecutor (created lazily).
    Note: Executor lock timing might be sensitive in complex multi-loop test scenarios, but generally safe.
    """
    global _telegram_executor
    if _telegram_executor is None:
        async with _executor_lock:
            if _telegram_executor is None:
                LOG.info("Creating dedicated ThreadPoolExecutor for Telegram alerts (max_workers=4).")
                _telegram_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="telegram_sender")

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(_telegram_executor, lambda: send_alert(**kwargs))
    except Exception as e:
         LOG.error("Error executing send_alert in thread pool: %s", e)
         return False

# ---------------------------------------------------------------------------
# Graceful Shutdown Handler (Unchanged from v1.3)
# ---------------------------------------------------------------------------
def _shutdown_executor():
    """Shutdown the global executor if it was created."""
    global _telegram_executor
    if _telegram_executor:
        LOG.info("Shutting down Telegram sender thread pool...")
        _telegram_executor.shutdown(wait=True, cancel_futures=True)
        LOG.info("Telegram sender thread pool shut down.")

atexit.register(_shutdown_executor)

# ---------------------------------------------------------------------------
# Tiny CLI helper / Test Area
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    def my_before_send(payload, _): print(f"  [CALLBACK] Before Send: Chat={payload.get('chat_id')}, Text Snippet='{str(payload.get('text',''))[:30]}...'")
    def my_after_send(payload, response):
        status = "OK" if response and response.get("ok") else "FAIL"
        desc = response.get("description", "N/A") if response else "N/A"
        print(f"  [CALLBACK] After Send: Status={status}, Desc='{desc}', Text Snippet='{str(payload.get('text',''))[:30]}...'")

    print("--- Telegram Alert Router Test v1.3.1 (Bug Fix) ---")
    test_token = _token_for()
    test_chat = _chat_for()
    if not test_token or not test_chat:
        print("[!] TELEGRAM_BOT_TOKEN_DEFAULT/TELEGRAM_CHAT_ID_DEFAULT (or fallbacks) not set in .env. Cannot run live test.")
    else:
        print(f"[*] Using Token: ...{test_token[-6:]}")
        print(f"[*] Using Chat ID: {test_chat}")

        print("\n[TEST WITH CALLBACKS]")
        ok_cb = send_alert("Testing message with callbacks.", category="callback_test", before_send_callback=my_before_send, after_send_callback=my_after_send)
        print(f"    Callback Test Result: {'OK' if ok_cb else 'FAIL'}")
        time.sleep(2)

        print("\n[1] Sending plain text alert...")
        ok1 = send_alert("This is a plain text test message from the router script.", parse_mode=None, escape_markdown=False)
        print(f"    Result: {'OK' if ok1 else 'FAIL'}")
        time.sleep(2)

        print("\n[2] Sending MarkdownV2 alert (with escaping)...")
        md_v2_msg = "Testing *MarkdownV2* with _special_ chars like [links](http://example.com) and `code`."
        ok2 = send_alert(md_v2_msg, category="test", parse_mode="MarkdownV2", escape_markdown=True)
        print(f"    Result: {'OK' if ok2 else 'FAIL'}")
        time.sleep(2)

        print("\n[3] Sending Legacy Markdown alert (with escaping)...")
        legacy_md_msg = "Testing *Legacy* `Markdown` with _stars_ and `backticks`."
        ok3 = send_alert(legacy_md_msg, category="test", parse_mode="Markdown", escape_markdown=True)
        print(f"    Result: {'OK' if ok3 else 'FAIL'}")
        time.sleep(2)

        print("\n[4] Sending Status Update...")
        ok4 = send_alert("This is just a status update.", category="status", is_status_update=True, parse_mode="MarkdownV2")
        print(f"    Result: {'OK' if ok4 else 'FAIL'}")
        time.sleep(2)

        print("\n[5] Sending Long Message (will be chunked)...")
        long_msg = "This is the first paragraph.\n\n" + ("This is a repeating line for testing chunking. " * 100) + "\n\nThis is the middle paragraph.\n\n" + ("Another repeating line, just to be sure. " * 100) + "\n\nFinal paragraph."
        ok5 = send_alert(long_msg, category="longtest", prefix="📜 Long Message Test 📜")
        print(f"    Result: {'OK' if ok5 else 'FAIL'}")
        time.sleep(2)

        print("\n[6] Sending INTENTIONALLY BAD MarkdownV2 (should retry as plain)...")
        bad_md_v2_msg = "This has an unclosed [bracket *and* maybe _other_ issues."
        ok6 = send_alert(bad_md_v2_msg, category="bad_md", parse_mode="MarkdownV2", escape_markdown=False)
        print(f"    Result: {'OK (Retry likely succeeded)' if ok6 else 'FAIL (Retry also failed?)'}")
        time.sleep(2)

        async def run_async_test():
            print("\n[7] Sending async alert...")
            ok_async = await send_alert_async("This is an *async* test message.", category="async_test", parse_mode="MarkdownV2")
            print(f"    Async Result: {'OK' if ok_async else 'FAIL'}")

        print("\n[*] Running async test...")
        try: asyncio.run(run_async_test())
        except RuntimeError as e: print(f"    Could not run async test (perhaps already in an event loop?): {e}")

    print("\n--- Test Complete (Executor will shut down on exit) ---")

# --- END OF FILE telegram_alert_router.py ---