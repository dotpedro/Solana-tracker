# --- START OF FILE kol_soleyes_scraper.py ---

# Standard library imports
import json
import time
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

# Third-party imports
# import requests # Keep for now, might be needed elsewhere eventually, but not for alerts
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

# Define a category for alerts from this script
ALERT_CATEGORY_KOL = "SO leyes_SCRAPER" # <<< CHANGE: Updated alert category

# --- Configuration ---
# <<< CHANGE: Updated Nitter URL for _soleyes >>>
NITTER_URL_TO_TRY_WITH_SELENIUM = "https://nitter.net/_soleyes" # Target _soleyes account
STATE_FILE = "soleyes_scraper_state.json" # <<< CHANGE: State file for _soleyes
SIGNALS_FILE = "soleyes_structured_signals.json" # <<< CHANGE: Signals file for _soleyes
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

# --- Scraper Core (Selenium) ---
def fetch_nitter_tweets():
    USER_AGENT_STRING = os.getenv("SELENIUM_USER_AGENT", USER_AGENT_PLACEHOLDER_CHECK)
    if USER_AGENT_STRING == USER_AGENT_PLACEHOLDER_CHECK:
        # log("🚨 (fetch_nitter_tweets) CRITICAL: User-Agent placeholder in use! Using generic fallback.")
        USER_AGENT_STRING = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        # Alert about placeholder usage is commented out as per original file structure

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
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            log(f"❌ Error initializing ChromeDriver with WebDriverManager: {e}. Attempting system ChromeDriver.")
            try:
                driver = webdriver.Chrome(options=options) # Fallback to system ChromeDriver
            except Exception as e2:
                log(f"❌❌ Failed to initialize ChromeDriver (system path): {e2}. Selenium will not work.")
                send_alert(
                    f"Selenium Critical Failure (KOL Scraper: _soleyes): Could not initialize ChromeDriver. Both WebDriverManager and system path attempts failed. Details: {e2}",
                    category=ALERT_CATEGORY_KOL,
                    is_status_update=False
                )
                return []

        log(f"ℹ️ WebDriver initialized.")
        driver.set_page_load_timeout(45)
        driver.set_script_timeout(30)

        log(f"ℹ️ Navigating to {NITTER_URL_TO_TRY_WITH_SELENIUM}...")
        driver.get(NITTER_URL_TO_TRY_WITH_SELENIUM)
        log(f"ℹ️ Page navigation initiated. Current URL: {driver.current_url}")
        log(f"ℹ️ Waiting for page to potentially load dynamic content (initial sleep)...")
        time.sleep(10)

        page_source = driver.page_source
        log(f"ℹ️ Page source fetched. Length: {len(page_source)}. Current URL after load: {driver.current_url}")

        if not page_source or len(page_source) < 1000:
            error_msg = f"Selenium got a very short/empty page source from {NITTER_URL_TO_TRY_WITH_SELENIUM} (Length: {len(page_source)}). Possible block, error, or wrong page."
            log(f"⚠️ {error_msg}")
            with open(debug_file_path, "w", encoding="utf-8") as f:
                f.write(f"<!-- Selenium fetched page. URL: {driver.current_url} -->\n")
                f.write(page_source if page_source else "<!-- Page source was empty or None -->")
            log(f"🧪 Dumped Selenium page source to {debug_file_path}")
            return []

        with open(debug_file_path, "w", encoding="utf-8") as f:
           f.write(page_source)
        log(f"🧪 Dumped full Selenium page source to {debug_file_path}. PLEASE INSPECT THIS FILE if parsing fails!")

        soup = BeautifulSoup(page_source, "html.parser")
        tweet_elements_selector = ".timeline-item"
        content_selector = ".tweet-content"
        link_selector = "a.tweet-link"
        date_selector = "span.tweet-date > a"

        log(f"ℹ️ Using main tweet selector for Selenium output: '{tweet_elements_selector}'")
        tweet_elements = soup.select(tweet_elements_selector)

        if not tweet_elements:
            error_msg = f"No elements found using selector '{tweet_elements_selector}' in Selenium's page source for {NITTER_URL_TO_TRY_WITH_SELENIUM}."
            log(f"⚠️ {error_msg} (HTML saved to {debug_file_path}).")
            log(f"⚠️ PLEASE INSPECT '{debug_file_path}' AND UPDATE CSS SELECTORS IN THE SCRIPT IF NITTER'S STRUCTURE HAS CHANGED.")
            return []

        log(f"✅ Found {len(tweet_elements)} potential tweet elements using selector '{tweet_elements_selector}'.")
        parsed_tweets = []
        expected_user_in_link = NITTER_URL_TO_TRY_WITH_SELENIUM.split("/")[-1]
        for i, t_element in enumerate(tweet_elements):
            content_tag = t_element.select_one(content_selector)
            link_tag = t_element.select_one(link_selector)
            date_tag = t_element.select_one(date_selector)

            if not (content_tag and link_tag and date_tag):
                log(f"⚠️ Tweet element #{i+1}: Missing one or more core components (content, link, or date tag). Skipping. HTML: {str(t_element)[:200]}...")
                continue

            href_val = link_tag.get("href")
            if not (href_val and f"/{expected_user_in_link}/status/" in href_val):
                log(f"⚠️ Tweet element #{i+1}: Link '{href_val}' does not appear to be a status link for {expected_user_in_link}. Skipping.")
                continue

            tweet_id_match = re.search(r"/status/(\d+)", href_val)
            if not tweet_id_match:
                log(f"⚠️ Tweet element #{i+1}: Could not extract tweet ID from href '{href_val}'. Skipping.")
                continue
            tweet_id = tweet_id_match.group(1)

            tweet_text = content_tag.get_text(separator=' ', strip=True)
            date_title = date_tag.get("title")
            if not date_title:
                log(f"⚠️ Tweet element #{i+1} (ID: {tweet_id}): Date title attribute missing. Skipping.")
                continue

            parsed_datetime = None
            cleaned_date_title = re.sub(r'\s*\([^)]*\)$', '', date_title).strip()
            datetime_formats = [
                "%b %d, %Y · %I:%M %p %Z", "%d %b %Y, %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S %Z",
                "%a %b %d %H:%M:%S %z %Y", "%b %d, %Y %I:%M %p", "%d %b %Y %H:%M:%S",
            ]
            for fmt in datetime_formats:
                try:
                    dt_obj = datetime.strptime(cleaned_date_title, fmt)
                    parsed_datetime = dt_obj.replace(tzinfo=timezone.utc) if dt_obj.tzinfo is None else dt_obj.astimezone(timezone.utc)
                    break
                except ValueError:
                    continue

            if parsed_datetime is None:
                log(f"⚠️ Tweet element #{i+1} (ID: {tweet_id}): Could not parse date string '{date_title}' (cleaned: '{cleaned_date_title}'). Trying relative time.")
                visible_date_text = date_tag.get_text(strip=True)
                now_utc = datetime.now(timezone.utc)
                if m := re.match(r"(\d+)s", visible_date_text): parsed_datetime = now_utc - timedelta(seconds=int(m.group(1)))
                elif m := re.match(r"(\d+)m", visible_date_text): parsed_datetime = now_utc - timedelta(minutes=int(m.group(1)))
                elif m := re.match(r"(\d+)h", visible_date_text): parsed_datetime = now_utc - timedelta(hours=int(m.group(1)))
                else:
                    log(f"   Could not parse relative time '{visible_date_text}' either. Skipping tweet {tweet_id}.")
                    continue

            parsed_tweets.append({
                "tweet_id": tweet_id,
                "tweet_text": tweet_text,
                "tweet_actual_timestamp": int(parsed_datetime.timestamp()),
            })

        if parsed_tweets:
            parsed_tweets.sort(key=lambda x: x["tweet_actual_timestamp"], reverse=True)
            log(f"✅ Successfully parsed and SORTED {len(parsed_tweets)} tweets from {NITTER_URL_TO_TRY_WITH_SELENIUM} using Selenium.")
        else:
            log(f"⚠️ Found {len(tweet_elements)} elements with '{tweet_elements_selector}' using Selenium, but failed to parse details for any. Check {debug_file_path}.")
        return parsed_tweets

    except TimeoutException:
        error_msg = f"Selenium TimeoutException while loading {NITTER_URL_TO_TRY_WITH_SELENIUM}."
        log(f"❌ {error_msg}")
        send_alert(f"Selenium Failure (_soleyes): {error_msg}", category=ALERT_CATEGORY_KOL, is_status_update=False)
    except WebDriverException as e:
        error_msg = f"Selenium WebDriverException for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {str(e).splitlines()[0]}"
        log(f"❌ {error_msg}")
        send_alert(f"Selenium Failure (_soleyes): {error_msg}", category=ALERT_CATEGORY_KOL, is_status_update=False)
    except Exception as e:
        error_msg = f"An unexpected error with Selenium for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {type(e).__name__}: {e}"
        log(f"❌ {error_msg}")
        import traceback; log(traceback.format_exc())
        send_alert(f"Unexpected Selenium Error (_soleyes): {error_msg}", category=ALERT_CATEGORY_KOL, is_status_update=False)
    finally:
        if driver:
            try:
                driver.quit()
                log("ℹ️ Selenium WebDriver quit.")
            except Exception as quit_err:
                 log(f"⚠️ Error trying to quit Selenium WebDriver: {quit_err}")
                 send_alert(f"Info (_soleyes): Error during Selenium WebDriver quit: {quit_err}", category=ALERT_CATEGORY_KOL, is_status_update=True)
    return []


# --- Signal Extraction + Deduplication ---
# <<< CHANGE: Rewritten extract_signals function for _soleyes format >>>
def extract_signals(tweet_obj):
    text = tweet_obj["tweet_text"]
    tweet_id = tweet_obj["tweet_id"]
    tweet_ts = tweet_obj["tweet_actual_timestamp"]
    now_timestamp = int(datetime.now(timezone.utc).timestamp())

    action = "mention"
    symbols = []
    primary_symbol = None
    contract_address = None
    kol_wallet = None
    followers_str = None # Not available for _soleyes format

    # 1. Extract Contract Address from MobyScreener URL first (most reliable for _soleyes)
    # Example URL: https://mobyscreener.com/solana/EFGdfZrG7HBjowA63DCzEMjdcyGXgqJVSifjCxJWpump/?ref=...
    moby_url_match = re.search(r'mobyscreener\.com/solana/([1-9A-HJ-NP-Za-km-z]{32,44})', text, re.IGNORECASE)
    if moby_url_match:
        contract_address = moby_url_match.group(1)

    # 2. Extract main signal: KOL, Action, Primary Symbol
    # Example Line: 💸 Assasin (@assasin_eth) just sold $584 of $wifsmile at $0.000024 (23K MC)
    # Example Line: 🐋 $MASK Whale just bought $494 of $0 at $0.000018 (18K MC)
    main_signal_pattern = re.compile(
        r"^(?:💸|🐋)\s*(?P<kol_info>.+?)\s+just\s+(?P<action_verb>sold|bought)\s+"
        r"\$[\d,.]+\s+of\s+(?P<symbol>\$(?:[A-Za-z0-9_]+|[0-9]))"  # Handles $SYMBOL and $0
        r"(?:\s+at\s+\S+\s*(?:\([^)]+\s*MC\))?)?",  # Optional "at price (MC)" part
        re.IGNORECASE
    )
    
    signal_match = main_signal_pattern.match(text)

    if signal_match:
        action_verb = signal_match.group("action_verb").lower()
        action = "buy" if action_verb == "bought" else "sell"
        
        kol_info_str = signal_match.group("kol_info").strip()
        # Parse KOL info: "Name (@handle)" or just "Name" (like "$MASK Whale")
        kol_name_handle_match = re.match(r"^(.*?)\s*\((@\S+)\)$", kol_info_str) # @handle can contain underscores, etc.
        if kol_name_handle_match:
            name = kol_name_handle_match.group(1).strip()
            handle = kol_name_handle_match.group(2).strip()
            kol_wallet = f"{name} ({handle})"
        else:
            kol_wallet = kol_info_str # e.g., "$MASK Whale"

        primary_symbol_candidate = signal_match.group("symbol")
        # Validate primary_symbol is not a pure monetary value like $1K, $2M
        if not re.fullmatch(r"\$[0-9.,]+[KkMmBb]?", primary_symbol_candidate, re.IGNORECASE):
            primary_symbol = primary_symbol_candidate
            if primary_symbol not in symbols:
                 symbols.append(primary_symbol)
        # else: primary_symbol_candidate was monetary, primary_symbol remains None for now.
        
    # 3. If CA not found from Moby URL, try to find a standalone one (fallback)
    # This is less likely needed given _soleyes's consistent Moby URL usage.
    if not contract_address:
        # Regex for a Base58 string (Solana address), not preceded by / or alphanum, not followed by - or alphanum
        ca_fallback_match = re.search(r'(?<![/\w])([1-9A-HJ-NP-Za-km-z]{32,44})(?![-\w])', text)
        if ca_fallback_match:
            # Check if this fallback CA is part of a URL (if moby_url_match failed somehow)
            # This is a basic check; complex URL structures might bypass it.
            if f"/{ca_fallback_match.group(1)}" not in text and f"={ca_fallback_match.group(1)}" not in text :
                 contract_address = ca_fallback_match.group(1)
                 log(f"  ℹ️ Used fallback CA extraction for tweet ID {tweet_id}: {contract_address}")


    # 4. Extract all other $SYMBOLS mentioned in the text
    all_raw_symbols = re.findall(r"\$([A-Za-z0-9_]+|[0-9])", text) # Matches $SYMBOL or $0
    for s_raw in all_raw_symbols:
        s_formatted = f"${s_raw}"
        if not re.fullmatch(r"\$[0-9.,]+[KkMmBb]?", s_formatted, re.IGNORECASE): # Filter out monetary amounts
            if s_formatted not in symbols:
                symbols.append(s_formatted)
    symbols = sorted(list(set(symbols)))

    # 5. Refine primary_symbol if it was initially None (e.g. main regex matched a monetary amount for symbol)
    #    or if the main signal pattern didn't match but other symbols were found.
    if not primary_symbol and symbols:
        # Select the first non-monetary symbol from the list as primary
        first_valid_symbol = next((s_item for s_item in symbols if not re.fullmatch(r"\$[0-9.,]+[KkMmBb]?", s_item, re.IGNORECASE)), None)
        if first_valid_symbol:
            primary_symbol = first_valid_symbol
            log(f"  ℹ️ Refined primary_symbol to '{primary_symbol}' for tweet ID {tweet_id} from general symbol scan.")

    # 6. Log warnings for incomplete buy/sell signals
    if action in ["buy", "sell"]:
        if not primary_symbol:
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}, Source: _soleyes): Action is '{action}' but no valid primary_symbol found. Text: {text[:100]}...")
        if not contract_address:
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}, Source: _soleyes): Action is '{action}' but no contract_address found. Text: {text[:100]}...")
        if not kol_wallet: # Should be set if action is buy/sell due to main regex structure
            log(f"⚠️ Signal Warning (Tweet ID: {tweet_id}, Source: _soleyes): Action is '{action}' but no kol_wallet (KOL name) found. Text: {text[:100]}...")
            
    return {
        "tweet_id": tweet_id,
        "tweet_text": text,
        "action": action,
        "symbols": symbols,
        "primary_symbol": primary_symbol,
        "contract_address": contract_address,
        "kol_wallet": kol_wallet,
        "follower_count_str": followers_str, # Will be None for _soleyes
        "authority_token": None, # Not applicable for _soleyes
        "timestamp_processed": now_timestamp,
        "tweet_actual_timestamp": tweet_ts,
        "source_account": "_soleyes" # Added for potential downstream disambiguation
    }


# --- Main Runner ---
def run_scraper():
    log(f"🚀 Starting KOL Twitter Scraper run (Source: _soleyes)...") # <<< CHANGE: Log source
    state = load_json_file(STATE_FILE)
    last_seen_id = state.get("last_processed_tweet_id")
    existing_signals_data = load_json_file(SIGNALS_FILE)
    existing_signals = existing_signals_data if isinstance(existing_signals_data, list) else []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    max_tweet_age_cutoff_ts = now_ts - (MAX_TWEET_AGE_HOURS_FALLBACK * 3600)
    signals_retention_cutoff_ts = now_ts - (SIGNALS_RETENTION_HOURS * 3600)

    log(f"🔩 Config: Last seen ID: {last_seen_id}, Max tweet age: {MAX_TWEET_AGE_HOURS_FALLBACK}h, Signal retention: {SIGNALS_RETENTION_HOURS}h")
    log(f"🎯 Target Nitter URL (_soleyes): {NITTER_URL_TO_TRY_WITH_SELENIUM}")


    tweets = []
    fetch_successful_flag = False
    try:
        tweets = fetch_nitter_tweets()
        if isinstance(tweets, list) and tweets:
            fetch_successful_flag = True
        elif isinstance(tweets, list) and not tweets:
            log("ℹ️ fetch_nitter_tweets returned an empty list for _soleyes. This might be due to no new tweets, a parsing issue, or Nitter/Selenium problems.")
            fetch_successful_flag = False
        else:
             log(f"⚠️ fetch_nitter_tweets (for _soleyes) returned unexpected type: {type(tweets)}. Treating as failure.")
             tweets = []
             fetch_successful_flag = False

    except Exception as e:
        log(f"❌ CRITICAL UNHANDLED error during _soleyes tweet fetching call: {e}")
        import traceback; log(traceback.format_exc())
        send_alert(
            f"Tweet Fetching System Error (_soleyes): {type(e).__name__}: {e}\nCheck logs.",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
        tweets = []
        fetch_successful_flag = False

    if not fetch_successful_flag:
        log("ℹ️ Tweet fetching for _soleyes did not return any processable tweets or failed this cycle.")
        send_alert(
            f"Tweet Fetching Issue from _soleyes ({NITTER_URL_TO_TRY_WITH_SELENIUM}): `fetch_nitter_tweets` returned no tweets or encountered an issue. Check script logs & debug HTML.",
            category=ALERT_CATEGORY_KOL,
            is_status_update=False
        )
    elif fetch_successful_flag:
        log(f"➡️ Fetched and sorted {len(tweets)} tweets for _soleyes processing.")
        # Optional: Log first few tweet timestamps (already in original code)

    new_signals = []
    processed_in_current_run_ids = set()
    newest_tweet_id_in_batch_overall = None
    if tweets:
        try:
             valid_tweet_ids = [int(t['tweet_id']) for t in tweets if isinstance(t.get('tweet_id'), str) and t['tweet_id'].isdigit()]
             if valid_tweet_ids: newest_tweet_id_in_batch_overall = str(max(valid_tweet_ids))
        except (ValueError, TypeError, KeyError) as e:
            log(f"⚠️ Error determining newest tweet ID from _soleyes batch: {e}. Tweets sample: {str(tweets[:2])[:200]}...")

    for tweet_idx, tweet in enumerate(tweets): # tweets are sorted newest first
        if not isinstance(tweet, dict) or "tweet_id" not in tweet or "tweet_actual_timestamp" not in tweet or "tweet_text" not in tweet:
             log(f"⚠️ Skipping invalid _soleyes tweet structure at index {tweet_idx}: {str(tweet)[:100]}...")
             continue
        # ... (rest of the processing loop is largely the same as original) ...
        tweet_id = tweet["tweet_id"]
        tweet_actual_ts = tweet["tweet_actual_timestamp"]

        if not isinstance(tweet_id, str) or not tweet_id.isdigit():
             log(f"⚠️ Skipping _soleyes tweet with invalid ID at index {tweet_idx}: ID={tweet_id}")
             continue
        if not isinstance(tweet_actual_ts, int):
             log(f"⚠️ Skipping _soleyes tweet {tweet_id} with invalid timestamp type: {type(tweet_actual_ts)}")
             continue

        if tweet_idx == 0 :
             log(f"ℹ️ Newest fetched _soleyes tweet: ID={tweet_id}, TS={datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()} vs age_cutoff_ts={datetime.fromtimestamp(max_tweet_age_cutoff_ts, tz=timezone.utc).isoformat()}")

        if tweet_actual_ts < max_tweet_age_cutoff_ts:
            log(f"ℹ️ _soleyes Tweet {tweet_id} (posted: {datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}) is older than {MAX_TWEET_AGE_HOURS_FALLBACK}h. Skipping.")
            break # Batch is sorted

        try:
            current_tweet_id_int = int(tweet_id)
            last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else None
            if last_seen_id_int is not None and current_tweet_id_int <= last_seen_id_int:
                log(f"ℹ️ _soleyes Tweet ID {tweet_id} is <= last_seen_id {last_seen_id}. Stopping.")
                break
        except (ValueError, TypeError):
             log(f"⚠️ Error comparing _soleyes tweet ID {tweet_id} with last_seen_id {last_seen_id}. Processing to be safe.")

        if tweet_id in processed_in_current_run_ids:
            log(f"ℹ️ _soleyes Tweet ID {tweet_id} already processed in this run. Skipping.")
            continue
        if any(s.get("tweet_id") == tweet_id for s in existing_signals):
            log(f"ℹ️ _soleyes Tweet {tweet_id} already in existing signals. Skipping.")
            continue

        log(f"Processing _soleyes tweet ID: {tweet_id}, Text: {tweet['tweet_text'][:120]}...")
        try:
            signal = extract_signals(tweet) # <<< This now calls the _soleyes specific extractor
            log(f"  Extracted Signal (_soleyes): Action='{signal['action']}', PriSym='{signal['primary_symbol']}', CA='{signal['contract_address']}', KOL='{signal['kol_wallet']}'")
            
            if signal['action'] in ['buy', 'sell'] and signal['primary_symbol'] and signal['contract_address'] and signal['kol_wallet']:
                new_signals.append(signal)
                processed_in_current_run_ids.add(tweet_id)
            elif signal['action'] in ['buy', 'sell']:
                log(f"  ⚠️ _soleyes signal for tweet {tweet_id} was '{signal['action']}' but missed critical info. Not adding. Text: {tweet['tweet_text'][:100]}")
            else:
                log(f"  ℹ️ _soleyes tweet {tweet_id} not a 'buy'/'sell' or missing info (Action: {signal['action']}). Skipping.")
        except Exception as extraction_err:
             log(f"❌ Error extracting signals from _soleyes tweet ID {tweet_id}: {extraction_err}")
             log(f"   Tweet Text: {tweet['tweet_text']}")

    # --- Update State and Signals File ---
    if new_signals:
        log(f"✨ Extracted {len(new_signals)} new valid signals from _soleyes.")
        # ... (rest of state update logic is same as original) ...
        processed_ids_int = [int(tid) for tid in processed_in_current_run_ids if tid.isdigit()]
        if processed_ids_int:
             newest_processed_id_in_run = str(max(processed_ids_int))
             log(f"📈 Newest successfully processed _soleyes tweet ID in this run: {newest_processed_id_in_run}")
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_processed_id_in_run) > last_seen_id_int:
                 state["last_processed_tweet_id"] = newest_processed_id_in_run
                 log(f"💾 State file for _soleyes will be updated with last_processed_tweet_id: {newest_processed_id_in_run}")
    elif tweets:
        log(f"ℹ️ No new signals extracted from {len(tweets)} fetched _soleyes tweets.")
        if newest_tweet_id_in_batch_overall:
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_tweet_id_in_batch_overall) > last_seen_id_int:
                 log(f"📈 Updating last_processed_tweet_id for _soleyes to {newest_tweet_id_in_batch_overall} (newest seen in batch).")
                 state["last_processed_tweet_id"] = newest_tweet_id_in_batch_overall

    final_signals = [s for s in existing_signals if isinstance(s, dict) and s.get("timestamp_processed", 0) >= signals_retention_cutoff_ts]
    log(f"🧼 Retained {len(final_signals)} signals for _soleyes after filtering old ones.")

    added_count = 0
    current_final_ids = {s.get("tweet_id") for s in final_signals if isinstance(s, dict) and "tweet_id" in s}
    for ns in new_signals:
        if isinstance(ns, dict) and "tweet_id" in ns and ns["tweet_id"] not in current_final_ids:
            final_signals.append(ns)
            current_final_ids.add(ns["tweet_id"])
            added_count += 1
    if added_count > 0:
        log(f"➕ Added {added_count} new unique _soleyes signals to the final list.")

    final_signals.sort(key=lambda s: s.get("tweet_actual_timestamp", 0), reverse=True)

    save_json_file(SIGNALS_FILE, final_signals) # Saves to sole_eyes_structured_signals.json
    save_json_file(STATE_FILE, state) # Saves to sole_eyes_scraper_state.json
    log(f"✅ _soleyes Scraper run finished. {added_count} new signals. {len(final_signals)} total in '{SIGNALS_FILE}'.")
    current_last_id_in_state = state.get("last_processed_tweet_id")
    log(f"💾 Final last_processed_tweet_id in _soleyes state: {current_last_id_in_state if current_last_id_in_state else 'Not set'}")


if __name__ == "__main__":
    SCRAPE_INTERVAL_SECONDS = int(os.getenv("SO leyes_SCRAPER_INTERVAL_SECONDS", "75")) # Default 75s, configurable

    log("🚀 KOL Twitter Scraper (_soleyes source) starting up...") # <<< CHANGE: Log source
    send_alert(
        "KOL Twitter Scraper (_soleyes source) script has initiated.", # <<< CHANGE: Alert source
        category=ALERT_CATEGORY_KOL,
        is_status_update=True
    )
    log("✅ Initiation alert sent for _soleyes scraper.")

    while True:
        log(f"\n========= Starting new _soleyes scrape cycle at {datetime.now(timezone.utc).isoformat()} =========")
        try:
            run_scraper()
            log(f"========= _soleyes scrape cycle finished. Waiting for {SCRAPE_INTERVAL_SECONDS} seconds... =========")
        except KeyboardInterrupt:
            log("🛑 _soleyes Scraper stopped by user (KeyboardInterrupt).")
            send_alert("KOL Scraper (_soleyes source) was manually stopped.", category=ALERT_CATEGORY_KOL, is_status_update=True)
            break
        except Exception as e:
            # ... (error handling largely same as original, just update logging/alerts for source) ...
            error_type = type(e).__name__
            error_details = str(e)
            log(f"❌❌❌ Unhandled CRITICAL exception in _soleyes scraper main loop: {error_type}: {error_details}")
            import traceback; tb_str = traceback.format_exc(); log(tb_str)
            alert_message = (
                f"KOL Scraper (_soleyes) Main Loop CRASH\n\n"
                f"Error Type: `{error_type}`\nDetails: `{error_details}`\n\n"
                f"Check logs. It will attempt to restart."
            )
            send_alert(message=alert_message, category=ALERT_CATEGORY_KOL, is_status_update=False)
            log(f"🕒 An unhandled error occurred in _soleyes scraper. Loop will sleep & retry.")
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- END OF FILE kol_soleyes_scraper.py ---