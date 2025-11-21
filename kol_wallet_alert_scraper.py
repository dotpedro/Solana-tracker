# --- START OF FILE kol_wallet_alert_scraper.py ---

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

# Define a category for alerts from this script
ALERT_CATEGORY_KOL = "KOL_WALLET_ALERT_SCRAPER" # <<<< CHANGED

# --- Configuration ---
NITTER_URL_TO_TRY_WITH_SELENIUM = "https://nitter.net/KOLWalletAlert" # <<<< CHANGED
STATE_FILE = "kol_wallet_alert_scraper_state.json" # <<<< CHANGED
SIGNALS_FILE = "kol_wallet_alert_structured_signals.json" # <<<< CHANGED
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
        USER_AGENT_STRING = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        # Alert for placeholder is commented out as per original file
        # send_alert(
        #     "User-Agent placeholder is in use for KOLWalletAlert scraper. Using a generic fallback. Update the script for potentially better results.",
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
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            log(f"❌ Error initializing ChromeDriver with WebDriverManager: {e}. Attempting system ChromeDriver.")
            try:
                driver = webdriver.Chrome(options=options)
            except Exception as e2:
                log(f"❌❌ Failed to initialize ChromeDriver (system path): {e2}. Selenium will not work.")
                send_alert(
                    f"Selenium Critical Failure ({account_name} scraper): Could not initialize ChromeDriver. Both WebDriverManager and system path attempts failed. Details: {e2}",
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
        for i, t_element in enumerate(tweet_elements):
            content_tag = t_element.select_one(content_selector)
            link_tag = t_element.select_one(link_selector)
            date_tag = t_element.select_one(date_selector)

            if not (content_tag and link_tag and date_tag):
                log(f"⚠️ Tweet element #{i+1}: Missing one or more core components (content, link, or date tag). Skipping. HTML: {str(t_element)[:200]}...")
                continue

            href_val = link_tag.get("href")
            expected_user_in_link = NITTER_URL_TO_TRY_WITH_SELENIUM.split("/")[-1]
            if not (href_val and f"/{expected_user_in_link}/status/" in href_val):
                log(f"⚠️ Tweet element #{i+1}: Link '{href_val}' does not appear to be a status link for user {expected_user_in_link}. Skipping.")
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
                    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                        parsed_datetime = dt_obj.replace(tzinfo=timezone.utc)
                    else:
                        parsed_datetime = dt_obj.astimezone(timezone.utc)
                    break
                except ValueError:
                    continue

            if parsed_datetime is None:
                log(f"⚠️ Tweet element #{i+1} (ID: {tweet_id}): Could not parse date string '{date_title}' (cleaned: '{cleaned_date_title}'). Trying relative time.")
                visible_date_text = date_tag.get_text(strip=True)
                now_utc = datetime.now(timezone.utc)
                m_s = re.match(r"(\d+)s", visible_date_text); m_m = re.match(r"(\d+)m", visible_date_text); m_h = re.match(r"(\d+)h", visible_date_text)
                if m_s: parsed_datetime = now_utc - timedelta(seconds=int(m_s.group(1)))
                elif m_m: parsed_datetime = now_utc - timedelta(minutes=int(m_m.group(1)))
                elif m_h: parsed_datetime = now_utc - timedelta(hours=int(m_h.group(1)))
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
            return parsed_tweets
        else:
            log(f"⚠️ Found {len(tweet_elements)} elements with '{tweet_elements_selector}' using Selenium, but failed to parse details for any. Check {debug_file_path}.")
            return []

    except TimeoutException:
        error_msg = f"Selenium TimeoutException while loading or interacting with {NITTER_URL_TO_TRY_WITH_SELENIUM}."
        log(f"❌ {error_msg}")
        send_alert(
            f"Selenium Failure ({account_name} scraper): {error_msg}\nCheck Nitter status and network. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL, is_status_update=False
        )
        return []
    except WebDriverException as e:
        error_msg = f"Selenium WebDriverException for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {str(e).splitlines()[0]}"
        log(f"❌ {error_msg}")
        send_alert(
            f"Selenium Failure ({account_name} scraper): {error_msg}\nThis could be a ChromeDriver issue or browser crash. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL, is_status_update=False
        )
        return []
    except Exception as e:
        error_msg = f"An unexpected error occurred with Selenium for {NITTER_URL_TO_TRY_WITH_SELENIUM}: {type(e).__name__}: {e}"
        log(f"❌ {error_msg}")
        import traceback; log(traceback.format_exc())
        send_alert(
            f"Unexpected Selenium Error ({account_name} scraper): {error_msg}\nCheck logs. URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}",
            category=ALERT_CATEGORY_KOL, is_status_update=False
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
                     f"Info ({account_name} scraper): Error during Selenium WebDriver quit: {quit_err}",
                     category=ALERT_CATEGORY_KOL, is_status_update=True
                 )
    return []


# --- Signal Extraction + Deduplication ---
# This function is largely the same as in kol_twitter_scraper.py,
# as the core tweet structure for KOLWalletAlert is similar to ai_xtn for these fields.
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
    followers_str = None

    # 1. Extract KOL Name and Follower Count
    # Example: "ohzarke, a KOL with 9.4K followers, just sold..."
    kol_match = re.search(
        r"([\w\.-]+),\s+a\s+KOL\s+with\s+([\d.,KkMmBb]+)\s+followers",
        text,
        re.IGNORECASE
    )
    if kol_match:
        kol_wallet = kol_match.group(1).strip()
        followers_str = kol_match.group(2).strip()
    else:
        # Fallback (less likely needed for KOLWalletAlert if format is consistent)
        potential_kol_start_match = re.match(r"^([\w\.-]{3,30})\s+(just\s+bought|just\s+sold|is\s+buying|is\s+selling)", text, re.IGNORECASE)
        if potential_kol_start_match:
            kol_wallet = potential_kol_start_match.group(1).strip()

    # 2. Extract primary signal: Action, Amount, Primary Symbol
    # Example: "...just sold $2.5K in $LetsBONK at $2.5M MC!"
    action_details_match = re.search(
        r"just\s+(bought|sold)\s+\$([\d.,KkMmBb]+)\s+in\s+(\$[A-Za-z][A-Za-z0-9_]{1,15})(?:\s+at\s+\$([\d.,KkMmBb]+)\s+MC)?",
        text,
        re.IGNORECASE
    )
    if action_details_match:
        action_verb = action_details_match.group(1).lower()
        action = "buy" if action_verb == "bought" else "sell"
        primary_symbol = action_details_match.group(3)
        if primary_symbol:
            symbols.append(primary_symbol)

    # 3. Extract Contract Address
    # Example: "CA: CDBdbNqmrLu1PcgjrFG52yxg71QnFhBZcUE6PSFdbonk"
    ca_match = re.search(r"CA:\s*([A-Za-z0-9]{30,60})", text, re.IGNORECASE)
    if ca_match:
        contract_address = ca_match.group(1)

    # 4. Extract all other $SYMBOLS (e.g., from "biggest win on $BUDDY")
    all_raw_symbols = re.findall(r"\$([A-Za-z][A-Za-z0-9_]{1,15})", text)
    for s_raw in all_raw_symbols:
        s_formatted = f"${s_raw}"
        if not re.fullmatch(r"\$[0-9.,]+[KMB]?", s_formatted, re.IGNORECASE):
            if s_formatted not in symbols:
                symbols.append(s_formatted)
    symbols = sorted(list(set(symbols)))

    if primary_symbol and re.fullmatch(r"\$[0-9.,]+[KMB]?", primary_symbol, re.IGNORECASE):
        non_amount_symbols_in_list = [s_item for s_item in symbols if not re.fullmatch(r"\$[0-9.,]+[KMB]?", s_item, re.IGNORECASE)]
        if non_amount_symbols_in_list:
            if primary_symbol != non_amount_symbols_in_list[0]:
                 primary_symbol = non_amount_symbols_in_list[0]
        else:
            primary_symbol = None

    if action in ["buy", "sell"]:
        if not primary_symbol:
            log(f"⚠️ Signal Warning (KOLWalletAlert Tweet ID: {tweet_id}): Action is '{action}' but no valid primary_symbol. Text: {text[:100]}...")
        if not contract_address:
            log(f"⚠️ Signal Warning (KOLWalletAlert Tweet ID: {tweet_id}): Action is '{action}' but no contract_address. Text: {text[:100]}...")
        if not kol_wallet:
            log(f"⚠️ Signal Warning (KOLWalletAlert Tweet ID: {tweet_id}): Action is '{action}' but no kol_wallet. Text: {text[:100]}...")

    return {
        "tweet_id": tweet_id,
        "tweet_text": text,
        "action": action,
        "symbols": symbols,
        "primary_symbol": primary_symbol,
        "contract_address": contract_address,
        "kol_wallet": kol_wallet,
        "follower_count_str": followers_str,
        "authority_token": None, # Not applicable for this source
        "timestamp_processed": now_timestamp,
        "tweet_actual_timestamp": tweet_ts
    }


# --- Main Runner ---
def run_scraper():
    log(f"🚀 Starting KOL Wallet Alert Scraper run (Source: {NITTER_URL_TO_TRY_WITH_SELENIUM})...")
    state = load_json_file(STATE_FILE)
    last_seen_id = state.get("last_processed_tweet_id")
    existing_signals_data = load_json_file(SIGNALS_FILE)
    existing_signals = existing_signals_data if isinstance(existing_signals_data, list) else []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    max_tweet_age_cutoff_ts = now_ts - (MAX_TWEET_AGE_HOURS_FALLBACK * 3600)
    signals_retention_cutoff_ts = now_ts - (SIGNALS_RETENTION_HOURS * 3600)

    log(f"🔩 Config: Last seen ID: {last_seen_id}, Max tweet age: {MAX_TWEET_AGE_HOURS_FALLBACK}h, Signal retention: {SIGNALS_RETENTION_HOURS}h")
    log(f"🎯 Target Nitter URL: {NITTER_URL_TO_TRY_WITH_SELENIUM}")

    tweets = []
    fetch_successful_flag = False
    try:
        tweets = fetch_nitter_tweets()
        if isinstance(tweets, list) and tweets:
            fetch_successful_flag = True
        elif isinstance(tweets, list) and not tweets:
            log("ℹ️ fetch_nitter_tweets returned an empty list. Possible reasons: no new tweets, parsing issue, or Nitter/Selenium problems.")
            fetch_successful_flag = False
        else:
             log(f"⚠️ fetch_nitter_tweets returned unexpected type: {type(tweets)}. Treating as failure.")
             tweets = []
             fetch_successful_flag = False
    except Exception as e:
        log(f"❌ CRITICAL UNHANDLED error during tweet fetching call: {e}")
        import traceback; log(traceback.format_exc())
        send_alert(
            f"Tweet Fetching System Error (KOLWalletAlert scraper): {type(e).__name__}: {e}\nCheck logs.",
            category=ALERT_CATEGORY_KOL, is_status_update=False
        )
        tweets = []
        fetch_successful_flag = False

    if not fetch_successful_flag:
        log(f"ℹ️ Tweet fetching did not return any processable tweets or failed for {NITTER_URL_TO_TRY_WITH_SELENIUM} this cycle.")
        send_alert(
            f"Tweet Fetching Issue from {NITTER_URL_TO_TRY_WITH_SELENIUM} (KOLWalletAlert scraper): `fetch_nitter_tweets` returned no tweets or encountered an issue. Check Nitter status, IP blocks, or Selenium/CSS selectors. See logs & debug HTML.",
            category=ALERT_CATEGORY_KOL, is_status_update=False
        )
    elif fetch_successful_flag:
        log(f"➡️ Fetched and sorted {len(tweets)} tweets for processing (from Selenium for {NITTER_URL_TO_TRY_WITH_SELENIUM}).")
        # Optional: Log first few tweet timestamps for debugging
        # for i, tweet_data in enumerate(tweets[:3]):
        #     log(f"   - Tweet #{i+1} ID={tweet_data.get('tweet_id')}, TS_UTC={datetime.fromtimestamp(tweet_data.get('tweet_actual_timestamp',0), tz=timezone.utc).isoformat()}")


    new_signals = []
    processed_in_current_run_ids = set()
    newest_tweet_id_in_batch_overall = None
    if tweets:
        try:
             valid_tweet_ids = [int(t['tweet_id']) for t in tweets if isinstance(t.get('tweet_id'), str) and t['tweet_id'].isdigit()]
             if valid_tweet_ids: newest_tweet_id_in_batch_overall = str(max(valid_tweet_ids))
        except Exception as e:
            log(f"⚠️ Error determining newest tweet ID from batch (KOLWalletAlert): {e}.")

    for tweet_idx, tweet in enumerate(tweets):
        if not isinstance(tweet, dict) or "tweet_id" not in tweet or "tweet_actual_timestamp" not in tweet or "tweet_text" not in tweet:
             log(f"⚠️ Skipping invalid tweet structure at index {tweet_idx} for KOLWalletAlert: {str(tweet)[:100]}...")
             continue
        tweet_id, tweet_actual_ts = tweet["tweet_id"], tweet["tweet_actual_timestamp"]
        if not (isinstance(tweet_id, str) and tweet_id.isdigit() and isinstance(tweet_actual_ts, int)):
             log(f"⚠️ Skipping KOLWalletAlert tweet with invalid ID/TS: ID={tweet_id}, TS_type={type(tweet_actual_ts)}")
             continue

        if tweet_idx == 0:
             log(f"ℹ️ Newest fetched KOLWalletAlert tweet: ID={tweet_id}, TS={datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}")

        if tweet_actual_ts < max_tweet_age_cutoff_ts:
            log(f"ℹ️ KOLWalletAlert Tweet {tweet_id} (posted: {datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}) is too old. Skipping remaining.")
            break

        try:
            current_tweet_id_int = int(tweet_id)
            last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else None
            if last_seen_id_int is not None and current_tweet_id_int <= last_seen_id_int:
                log(f"ℹ️ KOLWalletAlert Tweet ID {tweet_id} is <= last_seen_id {last_seen_id}. Stopping.")
                break
        except (ValueError, TypeError):
             log(f"⚠️ Error comparing KOLWalletAlert tweet ID {tweet_id} with last_seen_id {last_seen_id}. Processing to be safe.")

        if tweet_id in processed_in_current_run_ids or any(s.get("tweet_id") == tweet_id for s in existing_signals):
            log(f"ℹ️ KOLWalletAlert Tweet {tweet_id} already processed/exists. Skipping.")
            continue

        log(f"Processing KOLWalletAlert tweet ID: {tweet_id}, TS: {datetime.fromtimestamp(tweet_actual_ts, tz=timezone.utc).isoformat()}, Text: {tweet['tweet_text'][:120]}...")
        try:
            signal = extract_signals(tweet)
            log(f"  Extracted Signal (KOLWalletAlert): Action='{signal['action']}', Sym='{signal['primary_symbol']}', CA='{signal['contract_address']}', KOL='{signal['kol_wallet']}'")
            if signal['action'] in ['buy', 'sell'] and signal['primary_symbol'] and signal['contract_address'] and signal['kol_wallet']:
                new_signals.append(signal)
                processed_in_current_run_ids.add(tweet_id)
            elif signal['action'] in ['buy', 'sell']:
                log(f"  ⚠️ KOLWalletAlert Signal for tweet {tweet_id} was '{signal['action']}' but missed critical info. Not adding. Text: {tweet['tweet_text'][:100]}")
            else:
                log(f"  ℹ️ KOLWalletAlert Tweet {tweet_id} not 'buy'/'sell' or missing info (Action: {signal['action']}). Skipping.")
        except Exception as extraction_err:
             log(f"❌ Error extracting signals from KOLWalletAlert tweet ID {tweet_id}: {extraction_err}. Text: {tweet['tweet_text']}")

    if new_signals:
        log(f"✨ Extracted {len(new_signals)} new valid signals from KOLWalletAlert.")
        processed_ids_int = [int(tid) for tid in processed_in_current_run_ids if tid.isdigit()]
        if processed_ids_int:
             newest_processed_id_in_run = str(max(processed_ids_int))
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_processed_id_in_run) > last_seen_id_int:
                 state["last_processed_tweet_id"] = newest_processed_id_in_run
                 log(f"💾 State file for KOLWalletAlert will be updated with last_processed_tweet_id: {newest_processed_id_in_run}")
    elif tweets:
        log(f"ℹ️ No new signals extracted from {len(tweets)} fetched KOLWalletAlert tweets (duplicates, old, or non-actionable).")
        if newest_tweet_id_in_batch_overall:
             last_seen_id_int = int(last_seen_id) if last_seen_id and isinstance(last_seen_id, str) and last_seen_id.isdigit() else 0
             if int(newest_tweet_id_in_batch_overall) > last_seen_id_int:
                 log(f"📈 Updating last_processed_tweet_id for KOLWalletAlert to {newest_tweet_id_in_batch_overall} (newest seen in batch).")
                 state["last_processed_tweet_id"] = newest_tweet_id_in_batch_overall

    final_signals = [s for s in existing_signals if isinstance(s, dict) and s.get("timestamp_processed", 0) >= signals_retention_cutoff_ts]
    log(f"🧼 Retained {len(final_signals)} signals for KOLWalletAlert after filtering old ones.")

    current_final_ids = {s.get("tweet_id") for s in final_signals if isinstance(s, dict) and "tweet_id" in s}
    added_count = 0
    for ns in new_signals:
        if isinstance(ns, dict) and "tweet_id" in ns and ns["tweet_id"] not in current_final_ids:
            final_signals.append(ns)
            current_final_ids.add(ns["tweet_id"])
            added_count += 1
    if added_count > 0: log(f"➕ Added {added_count} new unique signals from KOLWalletAlert to the final list.")

    final_signals.sort(key=lambda s: s.get("tweet_actual_timestamp", 0), reverse=True)
    save_json_file(SIGNALS_FILE, final_signals)
    save_json_file(STATE_FILE, state)
    log(f"✅ KOLWalletAlert Scraper run finished. {added_count} new signals. {len(final_signals)} total signals in '{SIGNALS_FILE}'.")
    log(f"💾 Final last_processed_tweet_id in state for KOLWalletAlert: {state.get('last_processed_tweet_id', 'Not set')}")


if __name__ == "__main__":
    SCRAPE_INTERVAL_SECONDS = int(os.getenv("KOL_WALLET_ALERT_SCRAPER_INTERVAL_SECONDS", "75")) # <<<< CHANGED Default interval & ENV var

    log(f"🚀 KOL Wallet Alert Scraper (Source: {NITTER_URL_TO_TRY_WITH_SELENIUM}) starting up...") # <<<< CHANGED Log message
    send_alert(
        f"KOL Wallet Alert Scraper (Source: {NITTER_URL_TO_TRY_WITH_SELENIUM}) script has initiated.", # <<<< CHANGED Alert message
        category=ALERT_CATEGORY_KOL,
        is_status_update=True
    )
    log("✅ Initiation alert sent for KOL Wallet Alert Scraper.") # <<<< CHANGED Log message

    while True:
        log(f"\n========= Starting new KOL Wallet Alert scrape cycle at {datetime.now(timezone.utc).isoformat()} =========") # <<<< CHANGED Log message
        try:
            run_scraper()
            log(f"========= KOL Wallet Alert scrape cycle finished. Waiting {SCRAPE_INTERVAL_SECONDS}s... =========") # <<<< CHANGED Log message
        except KeyboardInterrupt:
            log("🛑 KOL Wallet Alert Scraper stopped by user (KeyboardInterrupt).") # <<<< CHANGED Log message
            send_alert(f"KOL Wallet Alert Scraper (Source: {NITTER_URL_TO_TRY_WITH_SELENIUM}) was manually stopped.", category=ALERT_CATEGORY_KOL, is_status_update=True) # <<<< CHANGED Alert message
            break
        except Exception as e:
            error_type = type(e).__name__; error_details = str(e)
            log(f"❌❌❌ Unhandled CRITICAL exception in KOL Wallet Alert main loop: {error_type}: {error_details}") # <<<< CHANGED Log message
            import traceback; tb_str = traceback.format_exc(); log(tb_str)
            alert_message = (
                f"KOL Wallet Alert Scraper ({NITTER_URL_TO_TRY_WITH_SELENIUM}) Main Loop CRASH\n\n" # <<<< CHANGED Alert message
                f"Error: `{error_type}: {error_details}`\n\n"
                f"Check logs. Will attempt restart."
            )
            send_alert(message=alert_message, category=ALERT_CATEGORY_KOL, is_status_update=False)
            log(f"🕒 Unhandled error in KOL Wallet Alert scraper. Loop will sleep {SCRAPE_INTERVAL_SECONDS}s and retry.") # <<<< CHANGED Log message
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- END OF FILE kol_wallet_alert_scraper.py ---