# --- START OF FILE kol_twitter_scraper.py ---

# Standard library imports
import json
import time
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

# Third-party imports
import requests # Keep for now, might be needed elsewhere eventually, but not for alerts
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import TimeoutException, WebDriverException

# Import the new alert router function
from telegram_alert_router import send_alert

# --- ENV & CONFIG ---
load_dotenv()
# TELEGRAM_BOT_TOKEN_KS and TELEGRAM_CHAT_ID_KS are removed - handled by router

# Define a category for alerts from this script
ALERT_CATEGORY_KOL = "KOL_SCRAPER"

# --- Configuration ---
# <<< CHANGE: Updated Nitter URL >>>
NITTER_URL_TO_TRY_WITH_SELENIUM = "https://nitter.net/ai_xtn" # Updated URL
STATE_FILE = "kol_scraper_state.json"
SIGNALS_FILE = "influencer_structured_signals.json"
SIGNALS_RETENTION_HOURS = 24
MAX_TWEET_AGE_HOURS_FALLBACK = 26 # How old a tweet can be to still be processed
USER_AGENT_PLACEHOLDER_CHECK = "PASTE_YOUR_BROWSER_EXACT_USER_AGENT_HERE"


# --- Utilities ---
def load_json_file(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            log(f"⚠️ Warning: Could not decode JSON from {path}. Returning empty dictionary.")
            return {}
    return {}

def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat(sep='T', timespec='milliseconds')}] {msg}")

# --- Telegram Alerting Utility REMOVED ---
# The function send_kol_scraper_alert is now removed.
# We will call send_alert from telegram_alert_router directly.

# --- Scraper Core (Selenium) ---
def fetch_nitter_tweets():
    USER_AGENT_STRING = os.getenv("SELENIUM_USER_AGENT", USER_AGENT_PLACEHOLDER_CHECK) # Load from .env or use placeholder
    if USER_AGENT_STRING == USER_AGENT_PLACEHOLDER_CHECK:
        # log("🚨 (fetch_nitter_tweets) CRITICAL: User-Agent placeholder in use! Using generic fallback.")
        USER_AGENT_STRING = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        # Alert about placeholder usage is commented out as per original file structure
        # send_alert(
        #     "User-Agent placeholder is in use. Using a generic fallback. Update the script for potentially better results.",
        #     category=ALERT_CATEGORY_KOL,
        #     is_status_update=True
        # )


    debug_html_dir = "nitter_debug_pages"
    os.makedirs(debug_html_dir, exist_ok=True)
    parsed_url = urlparse(NITTER_URL_TO_TRY_WITH_SELENIUM)
    instance_name = parsed_url.netloc.replace(":", "_") if parsed_url.netloc else "unknown_instance"
    account_name = NITTER_URL_TO_TRY_WITH_SELENIUM.split("/")[-1]
    debug_file_path = os.path.join(debug_html_dir, f"debug_selenium_{instance_name}_{account_name}.html")

    log(f"ℹ️ Attempting to fetch tweets from: {NITTER_URL_TO_TRY_WITH_SELENIUM} using Selenium.")
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument(f"user-agent={USER_AGENT_STRING}")
    options.add_argument("window-size=1920,1080")
    options.page_load_strategy = 'normal'
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')

    driver = None
    try:
        log(f"ℹ️ Setting up Selenium WebDriver...")
        # Ensure ChromeDriver is managed or path is set correctly in environment
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            log(f"❌ Error initializing ChromeDriver with WebDriverManager: {e}. Attempting system ChromeDriver.")
            # Fallback or specific path if WebDriverManager fails (ensure chromedriver is in PATH)
            try:
                driver = webdriver.Chrome(options=options)
            except Exception as e2:
                log(f"❌❌ Failed to initialize ChromeDriver (system path): {e2}. Selenium will not work.")
                send_alert(
                    f"Selenium Critical Failure: Could not initialize ChromeDriver. Both WebDriverManager and system path attempts failed. Details: {e2}",
                    category=ALERT_CATEGORY_KOL,
                    is_status_update=False
                )
                return []


        log(f"ℹ️ WebDriver initialized.")
        driver.set_page_load_timeout(45) # Increased timeout
        driver.set_script_timeout(30)   # Increased timeout

        log(f"ℹ️ Navigating to {NITTER_URL_TO_TRY_WITH_SELENIUM}...")
        driver.get(NITTER_URL_TO_TRY_WITH_SELENIUM)
        log(f"ℹ️ Page navigation initiated. Current URL: {driver.current_url}")
        log(f"ℹ️ Waiting for page to potentially load dynamic content (initial sleep)...")
        time.sleep(10) # Increased sleep for potentially slower Nitter instances or JS loading

        page_source = driver.page_source
        log(f"ℹ️ Page source fetched. Length: {len(page_source)}. Current URL after load: {driver.current_url}")

        if not page_source or len(page_source) < 1000: # Basic check for meaningful content
            error_msg = f"Selenium got a very short/empty page source from {NITTER_URL_TO_TRY_WITH_SELENIUM} (Length: {len(page_source)}). Possible block, error, or wrong page."
            log(f"⚠️ {error_msg}")
            with open(debug_file_path, "w", encoding="utf-8") as f:
                f.write(f"<!-- Selenium fetched page. URL: {driver.current_url} -->\n")
                f.write(page_source if page_source else "<!-- Page source was empty or None -->")
            log(f"🧪 Dumped Selenium page source to {debug_file_path}")
            # Alert for this specific failure condition will be handled in run_scraper
            return []

        with open(debug_file_path, "w", encoding="utf-8") as f:
           f.write(page_source)
        log(f"🧪 Dumped full Selenium page source to {debug_file_path}. PLEASE INSPECT THIS FILE if parsing fails!")

        soup = BeautifulSoup(page_source, "html.parser")
        # These selectors are standard for Nitter pages. If they fail, inspect debug_file_path.
        tweet_elements_selector = ".timeline-item" # Main container for a tweet
        content_selector = ".tweet-content"       # Contains the main text of the tweet
        link_selector = "a.tweet-link"            # The permalink to the tweet status
        date_selector = "span.tweet-date > a"     # Contains the timestamp in its title attribute

        log(f"ℹ️ Using main tweet selector for Selenium output: '{tweet_elements_selector}'")
        tweet_elements = soup.select(tweet_elements_selector)

        if not tweet_elements:
            error_msg = f"No elements found using selector '{tweet_elements_selector}' in Selenium's page source for {NITTER_URL_TO_TRY_WITH_SELENIUM}."
            log(f"⚠️ {error_msg} (HTML saved to {debug_file_path}).")
            log(f"⚠️ PLEASE INSPECT '{debug_file_path}' AND UPDATE CSS SELECTORS IN THE SCRIPT IF NITTER'S STRUCTURE HAS CHANGED.")
            # Alert for this specific failure condition will be handled in run_scraper
            return []

        log(f"✅ Found {len(tweet_elements)} potential tweet elements using selector '{tweet_elements_selector}'.")
        parsed_tweets = []
        for i, t_element in enumerate(tweet_elements):
            content_tag = t_element.select_one(content_selector)
            link_tag = t_element.select_one(link_selector) # This should be the link to the tweet status
            date_tag = t_element.select_one(date_selector) # This usually holds the date in its 'title'

            # Ensure all essential parts are found
            if not (content_tag and link_tag and date_tag):
                log(f"⚠️ Tweet element #{i+1}: Missing one or more core components (content, link, or date tag). Skipping. HTML: {str(t_element)[:200]}...")
                continue

            href_val = link_tag.get("href")
            if not href_val or "/status/" not in href_val : # Nitter links to actual tweets usually contain /status/
                # It might be a "Show this thread" or other non-tweet item.
                # For ai_xtn, the links are like "/ai_xtn/status/123456"
                # So we also check if the username is in the href before /status/
                expected_user_in_link = NITTER_URL_TO_TRY_WITH_SELENIUM.split("/")[-1]
                if not (href_val and f"/{expected_user_in_link}/status/" in href_val):
                    log(f"⚠️ Tweet element #{i+1}: Link '{href_val}' does not appear to be a status link. Skipping.")
                    continue


            tweet_id_match = re.search(r"/status/(\d+)", href_val)
            if not tweet_id_match:
                log(f"⚠️ Tweet element #{i+1}: Could not extract tweet ID from href '{href_val}'. Skipping.")
                continue
            tweet_id = tweet_id_match.group(1)

            tweet_text = content_tag.get_text(separator=' ', strip=True) # Use separator for multi-line content
            date_title = date_tag.get("title") # Nitter typically stores full date-time in title
            if not date_title:
                log(f"⚠️ Tweet element #{i+1} (ID: {tweet_id}): Date title attribute missing. Skipping.")
                continue

            parsed_datetime = None
            # Nitter date format is usually like "Apr 25, 2024 · 2:00 PM UTC" or "25 Apr 2024 14:00:00 UTC"
            # Sometimes it can be relative like "1h", but the title attribute should be absolute.
            cleaned_date_title = re.sub(r'\s*\([^)]*\)$', '', date_title).strip() # Remove (retweeted by X) if present
            datetime_formats = [
                "%b %d, %Y · %I:%M %p %Z",  # "Apr 25, 2024 · 2:00 PM UTC"
                "%d %b %Y, %H:%M:%S %Z",    # "25 Apr 2024, 14:00:00 UTC"
                "%Y-%m-%d %H:%M:%S %Z",    # "2024-04-25 14:00:00 UTC"
                "%a %b %d %H:%M:%S %z %Y",  # Fallback, e.g. from Python's datetime.strftime output
                # Nitter's specific format as observed, sometimes without explicit UTC but implied
                "%b %d, %Y %I:%M %p",      # "Apr 25, 2024 2:00 PM" (assume UTC)
                "%d %b %Y %H:%M:%S",        # "25 Apr 2024 14:00:00" (assume UTC)
            ]
            for fmt in datetime_formats:
                try:
                    dt_obj = datetime.strptime(cleaned_date_title, fmt)
                    # If timezone info is missing from the format string, assume UTC
                    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                        parsed_datetime = dt_obj.replace(tzinfo=timezone.utc)
                    else:
                        parsed_datetime = dt_obj.astimezone(timezone.utc) # Ensure it's UTC
                    break
                except ValueError:
                    continue

            if parsed_datetime is None:
                log(f"⚠️ Tweet element #{i+1} (ID: {tweet_id}): Could not parse date string '{date_title}' (cleaned: '{cleaned_date_title}'). Trying relative time from visible text.")
                # Fallback: Try to parse relative time from visible date text (e.g., "11s", "1m", "1h")
                # This is less reliable for precise timestamping but better than skipping.
                visible_date_text = date_tag.get_text(strip=True)
                now_utc = datetime.now(timezone.utc)
                relative_match_s = re.match(r"(\d+)s", visible_date_text)
                relative_match_m = re.match(r"(\d+)m", visible_date_text)
                relative_match_h = re.match(r"(\d+)h", visible_date_text)
                if relative_match_s:
                    parsed_datetime = now_utc - timedelta(seconds=int(relative_match_s.group(1)))
                elif relative_match_m:
                    parsed_datetime = now_utc - timedelta(minutes=int(relative_match_m.group(1)))
                elif relative_match_h:
                    parsed_datetime = now_utc - timedelta(hours=int(relative_match_h.group(1)))
                else: # Could be "Apr 25" if same year, etc. - harder to parse without full context
                    log(f"   Could not parse relative time '{visible_date_text}' either. Skipping tweet {tweet_id}.")
                    continue # Skip if date cannot be determined


            parsed_tweets.append({
                "tweet_id": tweet_id,
                "tweet_text": tweet_text,
                "tweet_actual_timestamp": int(parsed_datetime.timestamp()),
            })

        if parsed_tweets:
            parsed_tweets.sort(key=lambda x: x["tweet_actual_timestamp"], reverse=True) # Newest first
            log(f"✅ Successfully parsed and SORTED {len(parsed_tweets)} tweets from {NITTER_URL_TO_TRY_WITH_SELENIUM} using Selenium.")
            return parsed_tweets
        else:
            log(f"⚠️ Found {len(tweet_elements)} elements with '{tweet_elements_selector}' using Selenium, but failed to parse details for any. Check {debug_file_path}.")
            # Alert will be handled in run_scraper
            return []

    except TimeoutException:
        error_msg = f"Selenium TimeoutException while loading or interacting with {NITTER_URL_TO_TRY_WITH_SELENIUM}."
        log(f"❌ {error_msg}")
        send_alert(
            f"Selenium Failure: {error_msg}\nCheck Nitter status and network. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
        return []
    except WebDriverException as e:
        error_msg = f"Selenium WebDriverException for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {str(e).splitlines()[0]}"
        log(f"❌ {error_msg}")
        send_alert(
            f"Selenium Failure: {error_msg}\nThis could be a ChromeDriver issue or browser crash. Check logs. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
        return []
    except Exception as e:
        error_msg = f"An unexpected error occurred with Selenium for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {type(e).__name__}: {e}"
        log(f"❌ {error_msg}")
        import traceback
        log(traceback.format_exc())
        send_alert(
            f"Unexpected Selenium Error: {error_msg}\nCheck logs for full traceback. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
        return []
    finally:
        if driver:
            try:
                driver.quit()
                log("ℹ️ Selenium WebDriver quit.")
            except Exception as quit_err:
                 log(f"⚠️ Error trying to quit Selenium WebDriver: {quit_err}")
                 send_alert(
                     f"Info: Error encountered during Selenium WebDriver quit: {quit_err}",
                     category=ALERT_CATEGORY_KOL,
                     is_status_update=True
                 )
    return []


# --- Signal Extraction + Deduplication ---
# <<< CHANGE: Revamped extract_signals function >>>
def extract_signals(tweet_obj):
    text = tweet_obj["tweet_text"]
    tweet_id = tweet_obj["tweet_id"]
    tweet_ts = tweet_obj["tweet_actual_timestamp"]
    now_timestamp = int(datetime.now(timezone.utc).timestamp())

    # Initialize fields
    action = "mention" # Default, will be updated if specific patterns match
    symbols = []
    primary_symbol = None
    contract_address = None
    kol_wallet = None # This will store the KOL's name/handle
    followers_str = None
    # authority_token is not applicable for this new source format

    # 1. Extract KOL Name and Follower Count
    # Example: "eensx100, a KOL with 34.9K followers, just bought..."
    kol_match = re.search(
        r"([\w\.-]+),\s+a\s+KOL\s+with\s+([\d.,KkMmBb]+)\s+followers",
        text,
        re.IGNORECASE
    )
    if kol_match:
        kol_wallet = kol_match.group(1).strip()
        followers_str = kol_match.group(2).strip()
    else:
        # Fallback for cases where the "a KOL with X followers" phrase might be missing or different.
        # Try to extract a name from the beginning of the tweet if it looks like one.
        # This is a weaker match, more prone to errors for this new source.
        # The new source 'ai_xtn' is very structured, so this fallback might not be needed often.
        # Example: "Cupseyy just bought..." (if the follower part was missing)
        potential_kol_start_match = re.match(r"^([\w\.-]{3,30})\s+(just\s+bought|just\s+sold|is\s+buying|is\s+selling)", text, re.IGNORECASE)
        if potential_kol_start_match:
            kol_wallet = potential_kol_start_match.group(1).strip()
            # log(f"Debug (KOL): Fallback KOL match on name: {kol_wallet} for tweet ID {tweet_id}")

    # 2. Extract primary signal: Action, Amount, Primary Symbol
    # Example: "...just bought $549.8 in $Luna at $22.5K MC"
    # Example: "...just sold $4.0K in $INDICRATOR"
    # The "at $MarketCap MC" part is optional.
    action_details_match = re.search(
        r"just\s+(bought|sold)\s+\$([\d.,KkMmBb]+)\s+in\s+(\$[A-Za-z][A-Za-z0-9_]{1,15})(?:\s+at\s+\$([\d.,KkMmBb]+)\s+MC)?",
        text,
        re.IGNORECASE
    )

    if action_details_match:
        action_verb = action_details_match.group(1).lower()
        action = "buy" if action_verb == "bought" else "sell"
        # amount_str = action_details_match.group(2) # Not stored in current schema, but extracted
        primary_symbol = action_details_match.group(3)
        if primary_symbol:
            symbols.append(primary_symbol)
        # market_cap_str = action_details_match.group(4) # Optional, not stored in current schema
    # else: If this core pattern doesn't match, `action` remains "mention", `primary_symbol` is `None`.

    # 3. Extract Contract Address
    # Example: "CA: 2iM84cXF1rDjdWb7sxBsPZVbTPV7J4qVvSwvLCrkpump"
    ca_match = re.search(r"CA:\s*([A-Za-z0-9]{30,60})", text, re.IGNORECASE)
    if ca_match:
        contract_address = ca_match.group(1)

    # 4. Extract all other $SYMBOLS mentioned in the text (e.g., in "Holds" or "PnL" lines if they differ)
    # This ensures `symbols` list is comprehensive. `primary_symbol` is already set from the main action.
    all_raw_symbols = re.findall(r"\$([A-Za-z][A-Za-z0-9_]{1,15})", text)
    for s_raw in all_raw_symbols:
        s_formatted = f"${s_raw}"
        # Filter out symbols that are actually monetary amounts like $1K, $2.5M
        if not re.fullmatch(r"\$[0-9.,]+[KMB]?", s_formatted, re.IGNORECASE): # Use fullmatch for stricter amount check
            if s_formatted not in symbols:
                symbols.append(s_formatted)
    symbols = sorted(list(set(symbols))) # Ensure unique and sorted

    # If primary_symbol was determined but looks like an amount (e.g., $500k),
    # and there are other valid symbols, prefer a non-amount symbol.
    if primary_symbol and re.fullmatch(r"\$[0-9.,]+[KMB]?", primary_symbol, re.IGNORECASE):
        # log(f"Debug (PrimarySymbol): Primary symbol '{primary_symbol}' for tweet {tweet_id} looks like an amount.")
        non_amount_symbols_in_list = [s_item for s_item in symbols if not re.fullmatch(r"\$[0-9.,]+[KMB]?", s_item, re.IGNORECASE)]
        if non_amount_symbols_in_list:
            # If the original primary_symbol (amount) is different from the first non-amount symbol, update.
            if primary_symbol != non_amount_symbols_in_list[0]:
                 # log(f"Debug (PrimarySymbol): Replacing amount-like primary '{primary_symbol}' with '{non_amount_symbols_in_list[0]}'.")
                 primary_symbol = non_amount_symbols_in_list[0]
            # else: primary_symbol was already the only non-amount symbol or matches the first.
        else:
            # log(f"Debug (PrimarySymbol): Primary symbol '{primary_symbol}' is amount-like, but no other non-amount symbols found. Setting to None.")
            primary_symbol = None # Invalid primary symbol if it's an amount and no other candidates

    # If action is "buy" or "sell", but essential info (primary_symbol, contract_address, kol_wallet) is missing,
    # the signal is incomplete. runner_analyzer.py will filter these.
    # We log a warning here for easier debugging of parsing issues.
    if action in ["buy", "sell"]:
        if not primary_symbol:
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}): Action is '{action}' but no valid primary_symbol found. Text: {text[:100]}...")
        if not contract_address:
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}): Action is '{action}' but no contract_address found. Text: {text[:100]}...")
        if not kol_wallet:
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}): Action is '{action}' but no kol_wallet (KOL name) found. Text: {text[:100]}...")


    return {
        "tweet_id": tweet_id,
        "tweet_text": text,
        "action": action, # "buy", "sell", or "mention"
        "symbols": symbols,
        "primary_symbol": primary_symbol,
        "contract_address": contract_address,
        "kol_wallet": kol_wallet, # KOL's name/handle
        "follower_count_str": followers_str,
        "authority_token": None, # Not applicable for this source
        "timestamp_processed": now_timestamp,
        "tweet_actual_timestamp": tweet_ts
    }


# --- Main Runner ---
def run_scraper():
    log("🚀 Starting KOL Twitter Scraper run...")
    state = load_json_file(STATE_FILE)
    last_seen_id = state.get("last_processed_tweet_id")
    existing_signals_data = load_json_file(SIGNALS_FILE)
    # Ensure existing_signals is always a list, even if file is empty/malformed
    existing_signals = existing_signals_data if isinstance(existing_signals_data, list) else []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    max_tweet_age_cutoff_ts = now_ts - (MAX_TWEET_AGE_HOURS_FALLBACK * 3600)
    signals_retention_cutoff_ts = now_ts - (SIGNALS_RETENTION_HOURS * 3600)

    log(f"🔩 Config: Last seen ID: {last_seen_id}, Max tweet age: {MAX_TWEET_AGE_HOURS_FALLBACK}h (Cutoff: {datetime.fromtimestamp(max_tweet_age_cutoff_ts, tz=timezone.utc).isoformat()}), Signal retention: {SIGNALS_RETENTION_HOURS}h")
    log(f"🎯 Target Nitter URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}")


    tweets = []
    fetch_successful_flag = False # Renamed to avoid conflict with function name
    try:
        tweets = fetch_nitter_tweets() # This now returns [] on failure internally
        if isinstance(tweets, list) and tweets: # Explicitly check for non-empty list
            fetch_successful_flag = True
        elif isinstance(tweets, list) and not tweets:
            log("ℹ️ fetch_nitter_tweets returned an empty list. This might be due to no new tweets, a parsing issue, or Nitter/Selenium problems.")
            # This condition is now handled by the alert block below more generally
            fetch_successful_flag = False # Considered not successful if empty, as we expect new tweets periodically
        else:
             log(f"⚠️ fetch_nitter_tweets returned unexpected type: {type(tweets)}. Treating as failure.")
             tweets = [] # Ensure tweets is an empty list for subsequent logic
             fetch_successful_flag = False

    except Exception as e: # Catch errors during the *call* to fetch_nitter_tweets itself
        log(f"❌ CRITICAL UNHANDLED error during tweet fetching call: {e}")
        import traceback; log(traceback.format_exc())
        send_alert(
            f"Tweet Fetching System Error: {type(e).__name__}: {e}\nCheck logs.",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
        tweets = [] # Ensure tweets is empty list
        fetch_successful_flag = False

    # Alerting based on fetch outcome
    if not fetch_successful_flag: # This covers true errors, empty returns, or unexpected types
        log("ℹ️ Tweet fetching did not return any processable tweets or failed this cycle.")
        # Send an alert if fetch_nitter_tweets returned an empty list or failed.
        # Specific errors within fetch_nitter_tweets (Timeout, WebDriverException) already send alerts.
        # This alert covers cases where it returns [] without an internal exception (e.g., parse fail, no elements found).
        send_alert(
            f"Tweet Fetching Issue from {NITTER_URL_TO_TRY_WITH_SELENIUM}: `fetch_nitter_tweets` returned no tweets or encountered an issue. This could be due to: no new posts, Nitter instance problems, Selenium/CSS selector changes, or IP blocks. Check script logs & debug HTML file.",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False # Treat as a potential issue needing attention
        )
    elif fetch_successful_flag: # Only log this if tweets were actually fetched
        log(f"➡️ Fetched and sorted {len(tweets)} tweets for processing (from Selenium).")
        log(f"🔎 Inspecting first few fetched (and now sorted) tweet timestamps:")
        for i, tweet_data in enumerate(tweets[:5]):
            tweet_id = tweet_data.get("tweet_id")
            raw_ts = tweet_data.get("tweet_actual_timestamp")
            if isinstance(raw_ts, (int, float)):
                try:
                    parsed_dt_utc = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
                    log(f"   - Tweet #{i+1}: ID={tweet_id}, Timestamp (UTC)={parsed_dt_utc.isoformat()}")
                except (ValueError, OSError) as ts_err:
                     log(f"   - Tweet #{i+1}: ID={tweet_id}, Timestamp ERROR converting {raw_ts}: {ts_err}")
            else:
                log(f"   - Tweet #{i+1}: ID={tweet_id}, Timestamp (UTC)=N/A (raw_ts type: {type(raw_ts)}, value: {raw_ts})")

    new_signals = []
    processed_in_current_run_ids = set()
    newest_tweet_id_in_batch_overall = None
    if tweets:
        try:
             valid_tweet_ids = [int(t['tweet_id']) for t in tweets if isinstance(t.get('tweet_id'), str) and t['tweet_id'].isdigit()]
             if valid_tweet_ids:
                  newest_tweet_id_in_batch_overall = str(max(valid_tweet_ids))
        except (ValueError, TypeError, KeyError) as e:
            log(f"⚠️ Error determining newest tweet ID from batch: {e}. Tweets sample: {str(tweets[:2])[:200]}...")


    for tweet_idx, tweet in enumerate(tweets): # tweets are sorted newest first
        if not isinstance(tweet, dict) or "tweet_id" not in tweet or "tweet_actual_timestamp" not in tweet or "tweet_text" not in tweet:
             log(f"⚠️ Skipping invalid tweet structure at index {tweet_idx}: {str(tweet)[:100]}...")
             continue

        tweet_id = tweet["tweet_id"]
        tweet_actual_ts = tweet["tweet_actual_timestamp"]

        if not isinstance(tweet_id, str) or not tweet_id.isdigit():
             log(f"⚠️ Skipping tweet with invalid ID at index {tweet_idx}: ID={tweet_id}")
             continue
        if not isinstance(tweet_actual_ts, int):
             log(f"⚠️ Skipping tweet {tweet_id} with invalid timestamp type: {type(tweet_actual_ts)}")
             continue

        if tweet_idx == 0 : # Log comparison only for the first (newest) tweet
             log(f"ℹ️ Newest fetched tweet: ID={tweet_id}, TS={datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()} vs age_cutoff_ts={datetime.fromtimestamp(max_tweet_age_cutoff_ts, tz=timezone.utc).isoformat()}")

        if tweet_actual_ts < max_tweet_age_cutoff_ts:
            log(f"ℹ️ Tweet {tweet_id} (posted: {datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}) is older than {MAX_TWEET_AGE_HOURS_FALLBACK}h. Skipping this and any older tweets (batch is sorted).")
            break

        try: # Comparison with last_seen_id
            current_tweet_id_int = int(tweet_id)
            last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else None

            if last_seen_id_int is not None and current_tweet_id_int <= last_seen_id_int:
                log(f"ℹ️ Tweet ID {tweet_id} (int: {current_tweet_id_int}) is <= last_seen_id {last_seen_id} (int: {last_seen_id_int}). Stopping further processing for this batch.")
                break
        except (ValueError, TypeError):
             log(f"⚠️ Error comparing tweet ID {tweet_id} with last_seen_id {last_seen_id}. Processing tweet to be safe, but check state file.")
             # Continue processing this tweet; if it's old, the existing_signals check might catch it.

        if tweet_id in processed_in_current_run_ids:
            log(f"ℹ️ Tweet ID {tweet_id} already processed in this run (duplicate in fetched batch?). Skipping.")
            continue

        if any(s.get("tweet_id") == tweet_id for s in existing_signals):
            log(f"ℹ️ Tweet {tweet_id} already found in existing signals file (robustness check; should ideally be caught by last_seen_id). Skipping.")
            continue

        log(f"Processing tweet ID: {tweet_id}, Timestamp: {datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}, Text: {tweet['tweet_text'][:120]}...")
        try:
            signal = extract_signals(tweet)
            log(f"  Extracted Signal: Action='{signal['action']}', PrimarySymbol='{signal['primary_symbol']}', CA='{signal['contract_address']}', KOL='{signal['kol_wallet']}', Followers='{signal['follower_count_str']}'")
            
            # Add signal only if it's a buy/sell and has essential components
            if signal['action'] in ['buy', 'sell'] and signal['primary_symbol'] and signal['contract_address'] and signal['kol_wallet']:
                new_signals.append(signal)
                processed_in_current_run_ids.add(tweet_id)
            elif signal['action'] in ['buy', 'sell']:
                log(f"  ⚠️ Signal for tweet {tweet_id} was '{signal['action']}' but missed critical info (Symbol/CA/KOL). Not adding to new_signals. Text: {tweet['tweet_text'][:100]}")
            else: # Action is 'mention' or other
                log(f"  ℹ️ Tweet {tweet_id} not a 'buy' or 'sell' signal or missing critical info after extraction (Action: {signal['action']}). Skipping.")

        except Exception as extraction_err:
             log(f"❌ Error extracting signals from tweet ID {tweet_id}: {extraction_err}")
             log(f"   Tweet Text: {tweet['tweet_text']}")
             # send_alert(f"Signal extraction failed for tweet ID {tweet_id}: {extraction_err}", category=ALERT_CATEGORY_KOL, is_status_update=True)


    # --- Update State and Signals File ---
    if new_signals:
        log(f"✨ Extracted {len(new_signals)} new valid signals.")
        processed_ids_int = [int(tid) for tid in processed_in_current_run_ids if tid.isdigit()]
        if processed_ids_int:
             newest_processed_id_in_run = str(max(processed_ids_int))
             log(f"📈 Newest successfully processed tweet ID in this run: {newest_processed_id_in_run}")
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_processed_id_in_run) > last_seen_id_int:
                 state["last_processed_tweet_id"] = newest_processed_id_in_run
                 log(f"💾 State file will be updated with last_processed_tweet_id: {newest_processed_id_in_run}")
        else:
            log("ℹ️ No new signals were processed successfully (IDs issue?), state 'last_processed_tweet_id' remains unchanged.")

    elif tweets: # Tweets were fetched, but none resulted in *new* signals (e.g., all old or non-actionable)
        log(f"ℹ️ No new signals extracted from the {len(tweets)} fetched tweets (likely duplicates, too old, or not matching buy/sell criteria).")
        if newest_tweet_id_in_batch_overall:
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_tweet_id_in_batch_overall) > last_seen_id_int:
                 log(f"📈 Updating last_processed_tweet_id to {newest_tweet_id_in_batch_overall} as it's the newest tweet ID seen in this fetched batch.")
                 state["last_processed_tweet_id"] = newest_tweet_id_in_batch_overall
    # else: No tweets fetched, state 'last_processed_tweet_id' unchanged.

    final_signals = [s for s in existing_signals if isinstance(s, dict) and s.get("timestamp_processed", 0) >= signals_retention_cutoff_ts]
    log(f"🧼 Retained {len(final_signals)} signals after filtering old ones (cutoff: {datetime.fromtimestamp(signals_retention_cutoff_ts, tz=timezone.utc).isoformat()}).")

    current_final_ids = {s.get("tweet_id") for s in final_signals if isinstance(s, dict) and "tweet_id" in s}
    added_count = 0
    for ns in new_signals: # new_signals already filtered for buy/sell with essentials
        if isinstance(ns, dict) and "tweet_id" in ns and ns["tweet_id"] not in current_final_ids:
            final_signals.append(ns)
            current_final_ids.add(ns["tweet_id"])
            added_count += 1
    if added_count > 0:
        log(f"➕ Added {added_count} new unique signals to the final list.")

    final_signals.sort(key=lambda s: s.get("tweet_actual_timestamp", 0), reverse=True)

    save_json_file(SIGNALS_FILE, final_signals)
    save_json_file(STATE_FILE, state)
    log(f"✅ Scraper run finished. {added_count} new unique signals added. {len(final_signals)} total signals retained in '{SIGNALS_FILE}'.")
    current_last_id_in_state = state.get("last_processed_tweet_id")
    log(f"💾 Final last_processed_tweet_id in state: {current_last_id_in_state if current_last_id_in_state else 'Not set'}")


if __name__ == "__main__":
    SCRAPE_INTERVAL_SECONDS = int(os.getenv("KOL_SCRAPER_INTERVAL_SECONDS", "60")) # Default 1 minute, configurable

    log("🚀 KOL Twitter Scraper (ai_xtn source) starting up...")
    send_alert(
        "KOL Twitter Scraper (ai_xtn source) script has initiated.",
        category=ALERT_CATEGORY_KOL,
        is_status_update=True
    )
    log("✅ Initiation alert sent.")

    while True:
        log(f"\n========= Starting new scrape cycle at {datetime.now(timezone.utc).isoformat()} =========")
        try:
            run_scraper()
            log(f"========= Scrape cycle finished. Waiting for {SCRAPE_INTERVAL_SECONDS} seconds... =========")
        except KeyboardInterrupt:
            log("🛑 Scraper stopped by user (KeyboardInterrupt).")
            send_alert("KOL Scraper (ai_xtn source) was manually stopped (KeyboardInterrupt).", category=ALERT_CATEGORY_KOL, is_status_update=True)
            break
        except Exception as e:
            error_type = type(e).__name__
            error_details = str(e)
            log(f"❌❌❌ Unhandled CRITICAL exception in main loop: {error_type}: {error_details}")
            import traceback
            tb_str = traceback.format_exc()
            log(tb_str)
            alert_message = (
                f"KOL Scraper (ai_xtn) Main Loop CRASH\n\n"
                f"Error Type: `{error_type}`\n"
                f"Details: `{error_details}`\n\n"
                f"The scraper encountered a critical error. Check logs. It will attempt to restart the loop."
            )
            send_alert(message=alert_message, category=ALERT_CATEGORY_KOL, is_status_update=False)
            log(f"🕒 An unhandled error occurred. The loop will sleep for {SCRAPE_INTERVAL_SECONDS} seconds and then attempt to continue.")
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- END OF FILE kol_twitter_scraper.py ---