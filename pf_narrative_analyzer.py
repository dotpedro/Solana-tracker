
import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import openai
from dotenv import load_dotenv
import time
import re
from collections import Counter
import random
import asyncio
from zoneinfo import ZoneInfo # Python 3.9+
import traceback # Moved for wider scope

load_dotenv()

# --- Configuration ---
REPORTS_DIR = Path(r"C:\Users\joaob\Desktop\solana-tracker\narrative_reports") 
RAW_TOKEN_PREFIX = "raw_tokens_"
CATEGORIZED_TOKEN_PREFIX = "categorized_tokens_hourly_"
MIGRATED_TOKEN_PREFIX = "migrated_tokens_"
MINDSHARE_REPORT_PREFIX = "mindshare_report_hourly_"
LOG_TS_FORMAT = '%Y-%m-%d %H:%M:%S'
CATEGORIZATION_PROGRESS_LOG_FILE = REPORTS_DIR / "categorization_progress.json"

GPT_MODEL_FOR_SINGLE_CATEGORY = "gpt-3.5-turbo"
GPT_MODEL_FOR_BATCH_CATEGORY = "gpt-3.5-turbo"
GPT_MODEL_FOR_BONDED_CONTEXT = "gpt-3.5-turbo"
GPT_MODEL_FOR_REFINING_UNCLEAR = "gpt-4"

MAX_RAW_FILES_TO_SEARCH_FOR_ORIGINAL_DETAILS = 6
MAX_PRE_BOND_TOKENS_IN_GPT_PROMPT = 150
MAIN_LOOP_SLEEP_SECONDS = 10 # Sleep for categorization worker between passes
MINDSHARE_REPORT_INTERVAL_SECONDS = 1 * 60 
MINUTES_PAST_HOUR_FOR_MINDSHARE_CUTOFF = 1 

MAX_CONTEXT_EXAMPLES = 3
CONTEXT_ROLLING_WINDOW_SIZE = 30
MIN_TOKENS_FOR_UNCLEAR_REANALYSIS = 15
MAX_UNCLEAR_TOKENS_FOR_REANALYSIS_PROMPT = 100

MAX_CONCURRENT_GPT_BATCH_CALLS = 3 
TOKENS_PER_GPT_BATCH_CALL = 8      
RPD_HIT_COOLDOWN_SECONDS = 60 * 15 

# --- TIMEZONE CONFIGURATION ---
try:
    LOCAL_TZ = ZoneInfo("Europe/Lisbon") # Replace with your actual local timezone
except Exception as e_tz:
    print(f"[{datetime.now().strftime(LOG_TS_FORMAT)}] ⚠️ Could not load timezone 'Europe/Lisbon': {e_tz}. Defaulting to system local. This might cause issues if script and pf_newtoken.py are on different timezone settings.")
    LOCAL_TZ = None # Fallback to system local if ZoneInfo fails or not available

OPENAI_API_KEY_LOADED = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY_LOADED:
    print(f"[{datetime.now().strftime(LOG_TS_FORMAT)}] ❌ OPENAI_API_KEY not found. Exiting.")
    exit(1)

sync_client = openai.OpenAI(api_key=OPENAI_API_KEY_LOADED)
async_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY_LOADED)

RECENT_CATEGORIZATIONS_CONTEXT = []
rpd_hit_timestamps = {} 

# --- Utility Functions ---
def get_log_ts():
    # Use local time for general logging if LOCAL_TZ is set
    if LOCAL_TZ:
        return datetime.now(LOCAL_TZ).strftime(LOG_TS_FORMAT)
    return datetime.now().strftime(LOG_TS_FORMAT) # Fallback to naive local

def get_file_size(file_path: Path) -> int:
    try:
        return file_path.stat().st_size
    except FileNotFoundError:
        return -1

def is_past_hour(raw_filename: str, now_dt_local_naive: datetime) -> bool:
    # This function seems unused in the current logic flow but kept for completeness
    try:
        match = re.match(rf"^{RAW_TOKEN_PREFIX}(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}})\.json$", raw_filename)
        if not match:
            return False 
        
        file_hour_str = match.group(1)
        # Assumption: file_hour_str is UTC based on how filenames are generated.
        # If it were local, this function would need careful handling of timezones.
        file_dt_naive_utc = datetime.strptime(file_hour_str, '%Y-%m-%d_%H')
        
        # To compare correctly, make both timezone-aware or both naive in the same zone.
        # If now_dt_local_naive is truly local naive, and file is UTC naive:
        # This comparison is complex. Given it's unused, I'll leave it, but flag it as potentially problematic.
        # print(f"[{get_log_ts()}] is_past_hour: WARNING - this function's timezone logic might be flawed if used.")
        
        # Assuming raw_filename's hour is UTC and now_dt_local_naive is naive local time.
        # Convert file_dt_naive_utc to aware UTC, then to local, then to naive local for comparison.
        file_dt_aware_utc = file_dt_naive_utc.replace(tzinfo=timezone.utc)
        file_dt_aware_local = file_dt_aware_utc.astimezone(LOCAL_TZ if LOCAL_TZ else None) # Uses system local if LOCAL_TZ is None
        file_dt_local_naive_for_comparison = file_dt_aware_local.replace(tzinfo=None)

        return file_dt_local_naive_for_comparison < now_dt_local_naive.replace(minute=0, second=0, microsecond=0)
    except Exception as e:
        print(f"[{get_log_ts()}] is_past_hour: Exception parsing {raw_filename}: {e}")
        return False

# --- State Management ---
def load_categorization_progress() -> dict:
    if CATEGORIZATION_PROGRESS_LOG_FILE.exists():
        try:
            with open(CATEGORIZATION_PROGRESS_LOG_FILE, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    return json.loads(content)
        except Exception as e:
            print(f"[{get_log_ts()}] ⚠️ Error loading categorization progress: {e}. Starting fresh.")
            pass 
    return {}

def save_categorization_progress(progress_data: dict):
    try:
        with open(CATEGORIZATION_PROGRESS_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(progress_data, f, indent=2)
    except Exception as e:
        print(f"[{get_log_ts()}] ⚠️ Error saving categorization progress: {e}")

# --- Context Management for GPT Prompts ---
async def update_recent_categorizations_context_async(name, symbol, category):
    global RECENT_CATEGORIZATIONS_CONTEXT
    RECENT_CATEGORIZATIONS_CONTEXT.append({"name": name, "symbol": symbol, "category": category})
    if len(RECENT_CATEGORIZATIONS_CONTEXT) > CONTEXT_ROLLING_WINDOW_SIZE:
        RECENT_CATEGORIZATIONS_CONTEXT.pop(0)

def get_dynamic_examples_for_prompt() -> str:
    global RECENT_CATEGORIZATIONS_CONTEXT
    if not RECENT_CATEGORIZATIONS_CONTEXT: return ""
    selected_examples = []
    seen_categories_for_examples = set()
    context_copy = list(RECENT_CATEGORIZATIONS_CONTEXT) 
    for item in reversed(context_copy): 
        name, symbol, cat = item["name"], item["symbol"], item["category"]
        if cat not in seen_categories_for_examples and cat not in ["Error: GPT Category Call Failed", "Uncategorized by GPT", "Uncategorized (No Name/Symbol)", "Error: Daily Rate Limit Hit", "Error: Max Retries Rate Limit"]:
            selected_examples.append(f"  - Ex: '{name}' ({symbol}) was categorized as '{cat}'")
            seen_categories_for_examples.add(cat)
            if len(selected_examples) >= MAX_CONTEXT_EXAMPLES: break
    if selected_examples:
        return "\n\nCONTEXT from RECENTLY SEEN TOKENS (Consider these current trends):\n" + "\n".join(reversed(selected_examples))
    return ""

# --- GPT API Call Functions ---
async def async_gpt_get_categories_for_token_batch(token_batch: list[dict], raw_filename_for_rpd_check: str) -> list[str]:
    global rpd_hit_timestamps
    if not token_batch:
        return []

    if raw_filename_for_rpd_check in rpd_hit_timestamps:
        if time.time() < rpd_hit_timestamps[raw_filename_for_rpd_check] + RPD_HIT_COOLDOWN_SECONDS:
            return ["Error: RPD Cooldown Active"] * len(token_batch)
        else:
            print(f"[{get_log_ts()}] ✅ RPD cooldown ended for {raw_filename_for_rpd_check}.")
            if raw_filename_for_rpd_check in rpd_hit_timestamps:
                 try:
                    del rpd_hit_timestamps[raw_filename_for_rpd_check]
                 except KeyError: pass

    token_details_for_prompt = []
    for i, token_info in enumerate(token_batch):
        name = token_info.get('name', 'N/A').replace('"', "'").replace("\n", " ")
        symbol = token_info.get('symbol', 'N/A').replace('"', "'").replace("\n", " ")
        token_details_for_prompt.append(
            f"  {i + 1}. Name: \"{name}\", Symbol: \"{symbol}\""
        )

    dynamic_examples = get_dynamic_examples_for_prompt()
    prompt_system = "You are an AI assistant. For a list of Solana tokens (name, symbol), assign a single, most fitting category to EACH token. Consider recent trends provided in context. Example base categories: Dog Meme, Cat Meme, Political Figure, Celebrity, AI Related, Tech, Abstract, Pop Culture, Animal (Other), Food, Location, Event, Other Meme, Unclear/Generic. Respond with a JSON object containing a single key \"categories\" which is an array of strings, where each string is the category for the corresponding token. The array must have the same number of elements as the input token list."
    prompt_user = (
        f"{dynamic_examples}\n\n"
        f"CURRENT TOKENS TO CATEGORIZE (Respond with JSON like {{ \"categories\": [\"cat1\", \"cat2\", ...] }}):\n" +
        "\n".join(token_details_for_prompt)
    )

    max_retries = 2 
    base_delay = 3.0

    for attempt in range(max_retries):
        try:
            response = await async_client.chat.completions.create(
                model=GPT_MODEL_FOR_BATCH_CATEGORY,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": prompt_user}
                ],
                temperature=0.1,
                max_tokens=len(token_batch) * 30 + 100,
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content.strip()
            
            try:
                parsed_response = json.loads(content)
                categories = []
                if isinstance(parsed_response, dict) and "categories" in parsed_response and isinstance(parsed_response["categories"], list):
                    categories = parsed_response["categories"]
                elif isinstance(parsed_response, list):
                         categories = parsed_response
                elif isinstance(parsed_response, dict):
                    for val in parsed_response.values():
                        if isinstance(val, list) and all(isinstance(i, str) for i in val) and len(val) == len(token_batch):
                            categories = val
                            break
                
                if categories and isinstance(categories, list) and len(categories) == len(token_batch):
                    final_categories = []
                    for i, cat_str in enumerate(categories):
                        clean_cat = str(cat_str).strip().replace("\"", "")
                        if not clean_cat: clean_cat = "Uncategorized by GPT (Batch Empty)"
                        final_categories.append(clean_cat)
                        await update_recent_categorizations_context_async(token_batch[i].get("name",""), token_batch[i].get("symbol",""), clean_cat)
                    return final_categories
                else:
                    print(f"[{get_log_ts()}] ❌ GPT batch: MISMATCH for {raw_filename_for_rpd_check}. Expected {len(token_batch)}, Got {len(categories) if isinstance(categories, list) else 'N/A'}. Raw: {content[:250]}")
                    return ["Error: GPT Batch Output Mismatch"] * len(token_batch)

            except json.JSONDecodeError:
                print(f"[{get_log_ts()}] ❌ GPT batch: NON-JSON for {raw_filename_for_rpd_check} (Attempt {attempt+1}). Raw: {content[:250]}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(base_delay * (1.5 ** attempt))
                    continue 
                else: 
                    return ["Error: GPT Batch Non-JSON Response"] * len(token_batch)

        except openai.RateLimitError as e:
            if e.body and "RPD" in e.body.get("message", ""):
                print(f"[{get_log_ts()}] ❌ GPT Batch: RPD HIT for {raw_filename_for_rpd_check}. Error: {e}")
                rpd_hit_timestamps[raw_filename_for_rpd_check] = time.time()
                return ["Error: Daily Rate Limit Hit"] * len(token_batch)
            if attempt < max_retries - 1:
                delay = (base_delay * (2 ** attempt)) + random.uniform(0, 1) 
                print(f"[{get_log_ts()}] ⚠️ GPT Batch: RPM/TPM limit for {raw_filename_for_rpd_check} (Attempt {attempt+1}/{max_retries}). Retrying in {delay:.2f}s.")
                await asyncio.sleep(delay)
            else: 
                print(f"[{get_log_ts()}] ❌ GPT Batch: Max retries for RPM/TPM rate limit for {raw_filename_for_rpd_check}.")
                return ["Error: Max Retries Rate Limit"] * len(token_batch)
        except Exception as e:
            print(f"[{get_log_ts()}] ❌ GPT batch: UNEXPECTED error for {raw_filename_for_rpd_check} (Attempt {attempt+1}): {type(e).__name__} {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (1.5 ** attempt))
                continue
            else:
                 return ["Error: GPT Batch Call Failed"] * len(token_batch)
    return ["Error: GPT Batch All Retries Failed"] * len(token_batch)

def gpt_refine_unclear_categories(tokens_with_unclear_categories: list) -> dict:
    if not tokens_with_unclear_categories: return {}
    prompt_token_list = []
    for token_info in tokens_with_unclear_categories[:MAX_UNCLEAR_TOKENS_FOR_REANALYSIS_PROMPT]:
        prompt_token_list.append(f"- '{token_info.get('name', 'N/A')}' (Symbol: {token_info.get('symbol', 'N/A')}, Mint: {token_info.get('mint', 'N/A')}, Initially: '{token_info.get('gpt_initial_category', 'Unclear')}')")
    if not prompt_token_list: return {}
    prompt = (
        "The following tokens were initially assigned vague categories like 'Other Meme', 'Unclear/Generic', or 'Uncategorized'.\n"
        "Please analyze this list to identify if there are any common underlying themes or specific narratives shared by groups of 3 or more tokens. "
        "If you find such a group, suggest a NEW, MORE SPECIFIC category name for that group.\n\n"
        "LIST OF TOKENS WITH VAGUE CATEGORIES:\n" +
        "\n".join(prompt_token_list) + "\n\n"
        "Return your answer strictly as a JSON object where keys are the NEW SPECIFIC THEME NAMES you identified, "
        "and values are lists of 'MINT_ADDRESS' strings for tokens that belong to that new theme. "
        "Include a key 'still_unclear_or_unique_mints' for mints that still don't fit a clear new group of 3+.\n"
        "Example JSON Output:\n"
        "{\n"
        "  \"Specific Theme A (e.g., Biden Prostate Meme)\": [\"mint1\", \"mint2\", \"mint3\"],\n"
        "  \"Specific Theme B (e.g., AI Girlfriend Tokens)\": [\"mint4\", \"mint5\", \"mint6\", \"mint7\"],\n"
        "  \"still_unclear_or_unique_mints\": [\"mint8\", \"mint9\"]\n"
        "}"
    )
    print(f"[{get_log_ts()}] 🧠 GPT (Refine Unclear): Analyzing {len(prompt_token_list)} tokens for common themes...")
    text_response_for_error = "No response" 
    try:
        response = sync_client.chat.completions.create(
            model=GPT_MODEL_FOR_REFINING_UNCLEAR,
            messages=[
                {"role": "system", "content": "You are an AI assistant that finds common themes in a list of vaguely categorized tokens and suggests more specific categories in JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000
            # Removed: response_format={"type": "json_object"} for gpt-4 base model
        )
        text_response_for_error = response.choices[0].message.content.strip()
        
        # Try to extract JSON from markdown code blocks if present
        match = re.search(r"```json\s*(\{.*?\})\s*```", text_response_for_error, re.DOTALL)
        json_str = match.group(1) if match else text_response_for_error
        
        refined_categories_map = json.loads(json_str)
        print(f"[{get_log_ts()}] ✅ GPT (Refine Unclear) successful. Found {len(refined_categories_map)- (1 if 'still_unclear_or_unique_mints' in refined_categories_map else 0) } new themes.")
        return refined_categories_map
    except json.JSONDecodeError as je:
        err_msg = f"[{get_log_ts()}] ❌ GPT (Refine Unclear) response not valid JSON: {je}. Raw: {text_response_for_error[:300]}"
        print(err_msg)
        return {"error": "GPT refine response not valid JSON", "raw_response": text_response_for_error}
    except Exception as e:
        print(f"[{get_log_ts()}] ❌ GPT (Refine Unclear) call failed: {type(e).__name__} {e}") # Added type of exception
        return {"error": f"GPT refine call failed: {str(e)}"}

def load_migrated_mints_for_window(base_hour_dt: datetime, window_hours: int = 3) -> list[dict]:
    all_migration_events_in_window = []
    for i in range(window_hours + 1): 
        check_dt = base_hour_dt + timedelta(hours=i)
        migration_log_filename = REPORTS_DIR / f"{MIGRATED_TOKEN_PREFIX}{check_dt.strftime('%Y-%m-%d_%H')}.json"
        if migration_log_filename.exists():
            try:
                with open(migration_log_filename, "r", encoding="utf-8") as f:
                    events = json.load(f)
                    if isinstance(events, list):
                        all_migration_events_in_window.extend([e for e in events if isinstance(e, dict) and "mint" in e])
            except Exception as e: print(f"[{get_log_ts()}] 💥 Error loading/parsing {migration_log_filename.name}: {e}")
    return all_migration_events_in_window

def gpt_get_theme_and_related_pre_bond_from_categorized(
    target_bonded_token_info: dict,
    categorized_pre_bond_tokens_in_creation_hour: list
    ) -> dict:
    target_display_name = f"{target_bonded_token_info.get('symbol','???')} - {target_bonded_token_info.get('name','UnkName')}"
    target_theme = target_bonded_token_info.get("gpt_initial_category", "Unknown Theme")
    pre_bond_info_for_prompt = []
    sample_for_prompt = categorized_pre_bond_tokens_in_creation_hour
    sampling_note = ""
    if len(categorized_pre_bond_tokens_in_creation_hour) > MAX_PRE_BOND_TOKENS_IN_GPT_PROMPT:
        sample_for_prompt = random.sample(categorized_pre_bond_tokens_in_creation_hour, MAX_PRE_BOND_TOKENS_IN_GPT_PROMPT)
        sampling_note = f" (from a sample of {MAX_PRE_BOND_TOKENS_IN_GPT_PROMPT} of {len(categorized_pre_bond_tokens_in_creation_hour)} total pre-bond tokens this hour)"
    
    valid_sample_for_prompt = [t for t in sample_for_prompt if t.get("gpt_initial_category") and "Error:" not in t.get("gpt_initial_category")]

    for t in valid_sample_for_prompt:
        pre_bond_info_for_prompt.append(
            f"Token: '{t.get('symbol','???')} - {t.get('name','UnkName')}', Assigned Category: '{t.get('gpt_initial_category','Uncategorized')}'"
        )
    if not pre_bond_info_for_prompt:
         return {
            "final_target_token_theme_confirmation": target_theme,
            "related_pre_bond_tokens_by_gpt": [],
            "count_related_pre_bond_by_gpt": 0,
            "note": "No valid pre-bond tokens for context after filtering errors."
        }

    prompt = (
        f"The TARGET BONDED TOKEN is: '{target_display_name}'.\n"
        f"Its NARRATIVE/THEME (from a previous categorization step) is: '{target_theme}'.\n\n"
        f"From the following LIST OF PRE-BOND TOKENS (created in the same hour, with their pre-assigned categories){sampling_note}, "
        f"identify which ones are clearly part of the SAME NARRATIVE ('{target_theme}') or a VERY CLOSELY RELATED MEME THEME.\n"
        "Focus on strong thematic relevance. The pre-assigned categories are a hint, but make your own judgment on relatedness to the TARGET BONDED TOKEN's theme.\n\n"
        "PRE-BOND TOKENS LIST (WITH THEIR CATEGORIES):\n" +
        "\n".join(pre_bond_info_for_prompt) + "\n\n"
        "Return your answer strictly as a JSON object with the following keys:\n"
        "{\n"
        "  \"final_target_token_theme_confirmation\": \"<confirm or slightly refine the target_theme if necessary, otherwise repeat it>\",\n"
        "  \"related_pre_bond_tokens_by_gpt\": [\"SYMBOL - NAME (Category: <Its Category>)\", ...],\n"
        "  \"count_related_pre_bond_by_gpt\": <integer_count_of_tokens_in_the_list_above>\n"
        "}\n"
        "If no pre-bond tokens are related, 'related_pre_bond_tokens_by_gpt' should be an empty list and count 0."
    )
    text_response_for_error_bonded = "No response"
    try:
        response = sync_client.chat.completions.create(
            model=GPT_MODEL_FOR_BONDED_CONTEXT,
            messages=[{"role": "system", "content": "You are an AI assistant identifying related meme tokens based on themes, providing JSON output."},
                      {"role": "user", "content": prompt}],
            temperature=0.2, max_tokens=1500,
            response_format={"type": "json_object"} 
        )
        text_response_for_error_bonded = response.choices[0].message.content.strip()
        parsed = json.loads(text_response_for_error_bonded)
        if not all(k in parsed for k in ["final_target_token_theme_confirmation", "related_pre_bond_tokens_by_gpt", "count_related_pre_bond_by_gpt"]):
            return {"error": "GPT response missing keys", "raw_response": text_response_for_error_bonded, "parsed_json_attempt": parsed}
        if not isinstance(parsed["related_pre_bond_tokens_by_gpt"], list):
            parsed["related_pre_bond_tokens_by_gpt"] = []
        # Ensure count matches the list length, overriding GPT's count if necessary
        parsed["count_related_pre_bond_by_gpt"] = len(parsed["related_pre_bond_tokens_by_gpt"])
        return parsed
    except json.JSONDecodeError as je:
        err_msg = f"[{get_log_ts()}] ❌ GPT (Bonded Context) response not valid JSON: {je}. Raw: {text_response_for_error_bonded[:300]}"
        print(err_msg)
        return {"error": "GPT Bonded response not valid JSON", "raw_response": text_response_for_error_bonded}
    except Exception as e:
        return {"error": f"GPT call failed for '{target_display_name}': {str(e)}", "raw_response": text_response_for_error_bonded}

# --- Internal Helper for Categorization Passes ---
async def _process_file_batches(
    target_raw_file_path: Path, 
    raw_filename: str, 
    progress_data: dict, 
    tokens_to_process_in_file_slice: list, 
    current_processed_count_for_file: int, 
    last_ctx_reset_file: str,
    is_current_hour_file: bool = False 
    ):
    global RECENT_CATEGORIZATIONS_CONTEXT, rpd_hit_timestamps 
    log_ts = get_log_ts()
    categorized_any_new = False
    
    if not tokens_to_process_in_file_slice:
        all_tokens_in_raw_for_empty_slice_check = []
        if target_raw_file_path.exists(): 
            try:
                with open(target_raw_file_path, "r", encoding="utf-8") as f_check_empty:
                    content_check_empty = f_check_empty.read()
                    if content_check_empty.strip():
                        loaded_empty_check = json.loads(content_check_empty)
                        if isinstance(loaded_empty_check, list):
                            all_tokens_in_raw_for_empty_slice_check = loaded_empty_check
            except Exception as e_fcheck:
                print(f"[{log_ts}] _process_file_batches: Error reading {raw_filename} for empty slice check: {e_fcheck}")
        
        return False, last_ctx_reset_file 

    gpt_api_batches_to_prepare = []
    temp_current_batch = []
    invalid_tokens_in_this_pass = [] 

    for token_data in tokens_to_process_in_file_slice: 
        if not isinstance(token_data, dict) or not token_data.get("mint"):
            enriched_placeholder = {"name": token_data.get("name", "INVALID_DATA") if isinstance(token_data, dict) else "INVALID_DATA_TYPE", 
                                    "symbol": token_data.get("symbol", "INV_SYM") if isinstance(token_data, dict) else "INV_SYM_TYPE",
                                    "mint": token_data.get("mint", f"NO_MINT_{random.randint(1000,9999)}") if isinstance(token_data, dict) else f"NO_MINT_TYPE_{random.randint(1000,9999)}",
                                    "timestamp_utc": token_data.get("timestamp_utc", datetime.now(timezone.utc).isoformat()) if isinstance(token_data, dict) else datetime.now(timezone.utc).isoformat()}
            enriched_placeholder["gpt_initial_category"] = "Uncategorized (Invalid Raw Data)"
            invalid_tokens_in_this_pass.append(enriched_placeholder)
            continue 
        temp_current_batch.append(token_data)
        if len(temp_current_batch) == TOKENS_PER_GPT_BATCH_CALL:
            gpt_api_batches_to_prepare.append(list(temp_current_batch))
            temp_current_batch = []
    if temp_current_batch:
        gpt_api_batches_to_prepare.append(list(temp_current_batch))

    if not gpt_api_batches_to_prepare and not invalid_tokens_in_this_pass:
        # This case should ideally not be hit if tokens_to_process_in_file_slice was not empty
        # and contained valid tokens. If it only contained invalid tokens, invalid_tokens_in_this_pass would be populated.
        return False, last_ctx_reset_file 

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_GPT_BATCH_CALLS)
    async def manage_gpt_batch_call(token_sub_batch):
        async with semaphore:
            return await async_gpt_get_categories_for_token_batch(token_sub_batch, raw_filename)

    tasks = [manage_gpt_batch_call(batch) for batch in gpt_api_batches_to_prepare]
    results_of_batches = []
    if tasks:
        results_of_batches = await asyncio.gather(*tasks, return_exceptions=True)

    enriched_tokens_to_add_to_file = list(invalid_tokens_in_this_pass)
    total_tokens_attempted_in_this_run_via_gpt = 0
    rpd_hit_during_this_run = False

    for i, batch_categories_or_exception in enumerate(results_of_batches):
        original_token_sub_batch = gpt_api_batches_to_prepare[i]
        total_tokens_attempted_in_this_run_via_gpt += len(original_token_sub_batch)
        
        if isinstance(batch_categories_or_exception, Exception):
            print(f"[{log_ts}] Categorizer (_process_file_batches): Gather exception for {raw_filename}: {batch_categories_or_exception}")
            for token_data in original_token_sub_batch: enriched_tokens_to_add_to_file.append({**token_data.copy(), "gpt_initial_category": "Error: Batch Gather Exception"})
        elif isinstance(batch_categories_or_exception, list) and len(batch_categories_or_exception) == len(original_token_sub_batch):
            for token_idx, category_str in enumerate(batch_categories_or_exception):
                enriched_tokens_to_add_to_file.append({**original_token_sub_batch[token_idx].copy(), "gpt_initial_category": category_str})
                if category_str == "Error: Daily Rate Limit Hit": rpd_hit_during_this_run = True
        else: 
            print(f"[{log_ts}] Categorizer (_process_file_batches): Unexpected result for {raw_filename} ({len(original_token_sub_batch)} tokens): {str(batch_categories_or_exception)[:100]}")
            for token_data in original_token_sub_batch: enriched_tokens_to_add_to_file.append({**token_data.copy(), "gpt_initial_category": "Error: Unexpected Batch Result"})

    if enriched_tokens_to_add_to_file: # This covers both GPT processed and invalid tokens
        categorized_any_new = True
        hour_id_str = target_raw_file_path.stem.replace(RAW_TOKEN_PREFIX, "")
        categorized_filename = REPORTS_DIR / f"{CATEGORIZED_TOKEN_PREFIX}{hour_id_str}.json"
        current_categorized_tokens = []
        if categorized_filename.exists():
            try:
                with open(categorized_filename, "r", encoding="utf-8") as f_cat:
                    cat_content = f_cat.read()
                    if cat_content.strip(): current_categorized_tokens = json.loads(cat_content)
                if not isinstance(current_categorized_tokens, list): current_categorized_tokens = []
            except Exception as e_load_cat_existing: # More specific catch
                print(f"[{log_ts}] Categorizer (_process_file_batches): Error loading existing categorized file {categorized_filename.name}: {e_load_cat_existing}. Assuming empty.")
                current_categorized_tokens = []
        
        current_categorized_tokens.extend(enriched_tokens_to_add_to_file)
        
        try:
            with open(categorized_filename, "w", encoding="utf-8") as f_cat_write:
                json.dump(current_categorized_tokens, f_cat_write, indent=2)
            
            actual_tokens_processed_this_pass = len(enriched_tokens_to_add_to_file) # Simplified: this is the number newly added to categorized file
            new_processed_count = current_processed_count_for_file + actual_tokens_processed_this_pass
            
            progress_data.setdefault(raw_filename, {}).update({"processed_count": new_processed_count})
            
            if rpd_hit_during_this_run:
                progress_data[raw_filename]["rpd_hit_count"] = progress_data[raw_filename].get("rpd_hit_count", 0) + 1
                print(f"[{log_ts}] Categorizer (_process_file_batches): RPD limit for {raw_filename}. Pausing for this file.")
            
            save_categorization_progress(progress_data)

        except Exception as e_save_cat_batch:
            print(f"[{log_ts}] Categorizer (_process_file_batches): 💥 Error saving {categorized_filename.name}: {e_save_cat_batch}")
    
    return categorized_any_new, last_ctx_reset_file

# --- Main Categorization Orchestrator ---
async def continuously_categorize_pre_bond_tokens_async(progress_data: dict, last_processed_raw_file_for_context_reset: str | None):
    global RECENT_CATEGORIZATIONS_CONTEXT, rpd_hit_timestamps
    log_ts = get_log_ts()
    categorized_any_new_in_this_entire_call = False 

    now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    current_hour_file_name_str = f"{RAW_TOKEN_PREFIX}{now_utc_naive.strftime('%Y-%m-%d_%H')}.json"
    previous_hour_dt_calc = now_utc_naive - timedelta(hours=1)
    previous_hour_filename_str_with_ext = f"{RAW_TOKEN_PREFIX}{previous_hour_dt_calc.strftime('%Y-%m-%d_%H')}.json" 
    
    all_raw_files_sorted = sorted(
        [f for f in REPORTS_DIR.iterdir() if f.name.startswith(RAW_TOKEN_PREFIX) and f.is_file()],
        key=lambda x: x.name 
    )
    
    if not all_raw_files_sorted:
        return False, last_processed_raw_file_for_context_reset

    target_raw_file_path = None
    is_processing_current_hour_file_flag = False 

    # --- Part 1: Check & Process CURRENT hour's file actively ---
    current_hour_file_path_obj = REPORTS_DIR / current_hour_file_name_str
    if current_hour_file_path_obj.exists():
        raw_filename_current = current_hour_file_path_obj.name
        if not (raw_filename_current in rpd_hit_timestamps and \
           time.time() < rpd_hit_timestamps[raw_filename_current] + RPD_HIT_COOLDOWN_SECONDS):
            
            if raw_filename_current in rpd_hit_timestamps : 
                 print(f"[{get_log_ts()}] ✅ RPD cooldown just ended for current hour file {raw_filename_current}.")
                 try: del rpd_hit_timestamps[raw_filename_current]
                 except KeyError: pass
            
            file_progress_current = progress_data.get(raw_filename_current, {"processed_count": 0, "rpd_hit_count": 0})
            progress_data.setdefault(raw_filename_current, file_progress_current) 
            processed_count_for_current_file = file_progress_current.get("processed_count", 0)
            
            all_tokens_in_current_raw = []
            try:
                with open(current_hour_file_path_obj, "r", encoding="utf-8") as f_curr:
                    content_curr = f_curr.read()
                    if content_curr.strip(): 
                        loaded_curr = json.loads(content_curr)
                        if isinstance(loaded_curr, list): all_tokens_in_current_raw = loaded_curr
                if not isinstance(all_tokens_in_current_raw, list) and all_tokens_in_current_raw: 
                     print(f"[{log_ts}] Categorizer: 💥 Current raw file {raw_filename_current} content is not a list. Content: {str(all_tokens_in_current_raw)[:100]}")
                     all_tokens_in_current_raw = [] 
            except Exception as e_load_curr_active:
                print(f"[{log_ts}] Categorizer: 💥 Error loading/parsing current hour file {raw_filename_current}: {e_load_curr_active}.")
                all_tokens_in_current_raw = [] 
            
            if all_tokens_in_current_raw: 
                tokens_left_in_current_file = all_tokens_in_current_raw[processed_count_for_current_file:]
                if tokens_left_in_current_file:
                    target_raw_file_path = current_hour_file_path_obj 
                    is_processing_current_hour_file_flag = True
        
    if not target_raw_file_path: 
        for rp_older_check in all_raw_files_sorted:
            raw_fn_older_check = rp_older_check.name
            if raw_fn_older_check == current_hour_file_name_str: continue 

            file_progress_entry = progress_data.get(raw_fn_older_check, {})
            is_fully_cat_older_check = file_progress_entry.get("fully_categorized", False)
            
            if is_fully_cat_older_check: 
                temp_all_tokens_recheck_older = []
                if rp_older_check.exists():
                    try:
                        with open(rp_older_check, "r", encoding="utf-8") as f_recheck_older:
                            content_recheck_older = f_recheck_older.read()
                            if content_recheck_older.strip(): 
                                loaded_recheck_older = json.loads(content_recheck_older)
                                if isinstance(loaded_recheck_older, list): temp_all_tokens_recheck_older = loaded_recheck_older
                        recorded_proc_recheck_older = file_progress_entry.get("processed_count", 0)
                        if len(temp_all_tokens_recheck_older) > recorded_proc_recheck_older:
                            print(f"[{log_ts}] Categorizer: File {raw_fn_older_check} re-opened due to new tokens ({len(temp_all_tokens_recheck_older)} > {recorded_proc_recheck_older}).")
                            progress_data.setdefault(raw_fn_older_check, {}) 
                            progress_data[raw_fn_older_check]["fully_categorized"] = False
                            progress_data[raw_fn_older_check]["rpd_hit_count"] = 0 
                            if raw_fn_older_check in rpd_hit_timestamps: 
                                try: del rpd_hit_timestamps[raw_fn_older_check]
                                except KeyError: pass
                            is_fully_cat_older_check = False 
                    except Exception as e_recheck_older_loop:
                        print(f"[{log_ts}] Categorizer: Error re-checking older file {raw_fn_older_check}: {e_recheck_older_loop}")

            if not is_fully_cat_older_check:
                if raw_fn_older_check in rpd_hit_timestamps and \
                   time.time() < rpd_hit_timestamps[raw_fn_older_check] + RPD_HIT_COOLDOWN_SECONDS:
                    continue 
                target_raw_file_path = rp_older_check
                is_processing_current_hour_file_flag = False 
                break 
    
    if not target_raw_file_path:
        active_raw_filenames_set_early_exit = {f.name for f in all_raw_files_sorted} 
        stale_entries_early_exit = [fname for fname in list(progress_data.keys())
                         if fname.startswith(RAW_TOKEN_PREFIX) and fname not in active_raw_filenames_set_early_exit and progress_data[fname].get("fully_categorized")]
        if stale_entries_early_exit:
            for fname_early in stale_entries_early_exit:
                if fname_early in progress_data: 
                    try: del progress_data[fname_early]
                    except KeyError: pass
            if stale_entries_early_exit: save_categorization_progress(progress_data)
        return False, last_processed_raw_file_for_context_reset

    raw_filename = target_raw_file_path.name 
    if last_processed_raw_file_for_context_reset != raw_filename: 
        print(f"[{log_ts}] Categorizer: Processing file {raw_filename}. Resetting GPT context.")
        RECENT_CATEGORIZATIONS_CONTEXT = []
        last_processed_raw_file_for_context_reset = raw_filename
        
    file_progress = progress_data.get(raw_filename, {"processed_count": 0, "rpd_hit_count": 0})
    if raw_filename not in progress_data: 
        progress_data[raw_filename] = file_progress 
    processed_count_for_file = file_progress.get("processed_count", 0)
    
    all_tokens_in_target_raw = []
    try:
        with open(target_raw_file_path, "r", encoding="utf-8") as f_target_load:
            content_target = f_target_load.read()
            if content_target.strip(): 
                loaded_target = json.loads(content_target)
                if isinstance(loaded_target, list): all_tokens_in_target_raw = loaded_target
        if not isinstance(all_tokens_in_target_raw, list) and all_tokens_in_target_raw: 
            print(f"[{log_ts}] Categorizer: 💥 Target raw file {raw_filename} content is not a list. Content: {str(all_tokens_in_target_raw)[:100]}")
            all_tokens_in_target_raw = [] 
    except Exception as e_load_target:
        print(f"[{log_ts}] Categorizer: 💥 Error loading/parsing target file {raw_filename}: {e_load_target}.")
        progress_data.setdefault(raw_filename,{}).update({"error_loading_raw": str(e_load_target)})
        save_categorization_progress(progress_data)
        hour_id_str_err_target = target_raw_file_path.stem.replace(RAW_TOKEN_PREFIX, "")
        cat_fn_err_target = REPORTS_DIR / f"{CATEGORIZED_TOKEN_PREFIX}{hour_id_str_err_target}.json"
        if not cat_fn_err_target.exists():
            try:
                with open(cat_fn_err_target, "w", encoding='utf-8') as f_err_cat_target: json.dump([], f_err_cat_target)
            except Exception as e_write_empty_cat_err: 
                print(f"[{log_ts}] Categorizer: Error writing empty categorized file for error case {cat_fn_err_target.name}: {e_write_empty_cat_err}")
    else: 
        tokens_left_in_target_file = all_tokens_in_target_raw[processed_count_for_file:]
        if tokens_left_in_target_file:
            slice_for_processing = tokens_left_in_target_file[:(MAX_CONCURRENT_GPT_BATCH_CALLS * TOKENS_PER_GPT_BATCH_CALL)]
            
            processed_this_pass_for_target, new_ctx_reset_file_target = \
                await _process_file_batches(target_raw_file_path, raw_filename, progress_data, 
                                            slice_for_processing, processed_count_for_file, 
                                            last_processed_raw_file_for_context_reset, 
                                            is_current_hour_file=is_processing_current_hour_file_flag) 
            if processed_this_pass_for_target:
                categorized_any_new_in_this_entire_call = True
            last_processed_raw_file_for_context_reset = new_ctx_reset_file_target
        
        elif not tokens_left_in_target_file: 
            hour_id_str_target_ensure = target_raw_file_path.stem.replace(RAW_TOKEN_PREFIX, "")
            cat_fn_target_ensure = REPORTS_DIR / f"{CATEGORIZED_TOKEN_PREFIX}{hour_id_str_target_ensure}.json"

            raw_is_empty_and_unprocessed = (len(all_tokens_in_target_raw) == 0 and processed_count_for_file == 0)
            all_raw_tokens_processed = (processed_count_for_file >= len(all_tokens_in_target_raw) and len(all_tokens_in_target_raw) > 0) 

            if not cat_fn_target_ensure.exists() and (raw_is_empty_and_unprocessed or all_raw_tokens_processed):
                try:
                    with open(cat_fn_target_ensure, "w", encoding="utf-8") as f_target_ensure:
                        json.dump([], f_target_ensure) 
                except Exception as e_create_cat:
                    print(f"[{log_ts}] Categorizer: Error ensuring categorized file {cat_fn_target_ensure.name}: {e_create_cat}")
    
    prev_hour_path_final_obj = REPORTS_DIR / previous_hour_filename_str_with_ext
    
    end_of_previous_hour_for_finalization = previous_hour_dt_calc.replace(minute=0,second=0,microsecond=0) + timedelta(hours=1)
    finalization_cutoff_for_previous_hour = end_of_previous_hour_for_finalization + timedelta(minutes=(MINUTES_PAST_HOUR_FOR_MINDSHARE_CUTOFF // 2))

    if now_utc_naive >= finalization_cutoff_for_previous_hour:
        if prev_hour_path_final_obj.exists() and \
           not progress_data.get(previous_hour_filename_str_with_ext, {}).get("fully_categorized", False):
            
            all_tokens_prev_hour_final_check = []
            try:
                with open(prev_hour_path_final_obj, "r", encoding="utf-8") as f_prev_final_check:
                    content_prev_final_check = f_prev_final_check.read()
                    if content_prev_final_check.strip(): 
                        loaded_prev_check = json.loads(content_prev_final_check)
                        if isinstance(loaded_prev_check, list): all_tokens_prev_hour_final_check = loaded_prev_check
                
                processed_count_prev_final_check = progress_data.get(previous_hour_filename_str_with_ext, {}).get("processed_count", 0)
                
                if processed_count_prev_final_check >= len(all_tokens_prev_hour_final_check):
                    print(f"[{log_ts}] Categorizer: Finalizing previous hour file {previous_hour_filename_str_with_ext} as fully categorized. Processed: {processed_count_prev_final_check}, Total in raw: {len(all_tokens_prev_hour_final_check)}.")
                    progress_data.setdefault(previous_hour_filename_str_with_ext, {})["fully_categorized"] = True
                    save_categorization_progress(progress_data)
                    
                    hour_id_prev_final_ensure = previous_hour_dt_calc.strftime('%Y-%m-%d_%H') 
                    cat_fn_prev_final_ensure = REPORTS_DIR / f"{CATEGORIZED_TOKEN_PREFIX}{hour_id_prev_final_ensure}.json"
                    if not cat_fn_prev_final_ensure.exists(): 
                        try:
                            with open(cat_fn_prev_final_ensure, "w", encoding='utf-8') as fc_prev_ensure: json.dump([], fc_prev_ensure)
                        except Exception as e_ensure_cat:
                            print(f"[{log_ts}] Categorizer: Error ensuring categorized file {cat_fn_prev_final_ensure.name} for finalized raw file: {e_ensure_cat}")
            except Exception as e_finalize_prev_loop:
                print(f"[{log_ts}] Categorizer: Error during finalization check for {previous_hour_filename_str_with_ext}: {e_finalize_prev_loop}")

    active_raw_filenames_set = {f.name for f in all_raw_files_sorted} 
    stale_entries = [fname for fname in list(progress_data.keys())
                     if fname.startswith(RAW_TOKEN_PREFIX) and fname not in active_raw_filenames_set and progress_data[fname].get("fully_categorized")]
    if stale_entries:
        for fname_stale in stale_entries: 
            if fname_stale in progress_data: 
                try:
                    del progress_data[fname_stale]
                except KeyError: pass 
        if stale_entries: save_categorization_progress(progress_data)

    return categorized_any_new_in_this_entire_call, last_processed_raw_file_for_context_reset

# --- Mindshare Report Generation ---
def generate_hourly_mindshare_report_from_categorized(categorized_token_file_path: Path, progress_data: dict):
    report_hour_id_str = categorized_token_file_path.stem.replace(CATEGORIZED_TOKEN_PREFIX, "")
    log_ts = get_log_ts()

    all_categorized_tokens_this_hour = []
    try:
        with open(categorized_token_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            if content.strip(): 
                loaded_data = json.loads(content)
                if isinstance(loaded_data, list):
                    all_categorized_tokens_this_hour = loaded_data
            else: 
                all_categorized_tokens_this_hour = [] 
    except FileNotFoundError:
        print(f"[{log_ts}] ⚠️ Mindshare Gen: Categorized file {categorized_token_file_path.name} not found. Skipping.")
        return False
    except json.JSONDecodeError as je:
        print(f"[{log_ts}] ⚠️ Mindshare Gen: Error decoding JSON from {categorized_token_file_path.name}: {je}. Skipping.")
        return False 
    except Exception as e:
        print(f"[{log_ts}] ⚠️ Mindshare Gen: Error loading {categorized_token_file_path.name}: {e}. Skipping.")
        return False

    if not all_categorized_tokens_this_hour:
        empty_report_data = {
            "report_generation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "target_creation_hour_id": report_hour_id_str, 
            "total_tokens_categorized_this_hour": 0,
            "hourly_mindshare_alert": f"Hour {report_hour_id_str} (UTC): No tokens found for analysis (raw file was empty or contained no processable tokens).",
            "pre_bond_category_distribution_after_refinement": {},
            "total_bonded_tokens_from_this_hour_analyzed": 0,
            "dominant_bonded_narratives_with_strength": {},
            "potential_outliers_among_bonded": [],
            "detailed_bonded_token_analyses": []
        }
        report_filename = REPORTS_DIR / f"{MINDSHARE_REPORT_PREFIX}{report_hour_id_str}.json"
        try:
            with open(report_filename, "w", encoding="utf-8") as f:
                json.dump(empty_report_data, f, ensure_ascii=False, indent=2)
            print(f"[{log_ts}] 📝 Mindshare Gen: Minimal mindshare report saved for empty hour: {report_filename.name}")
            
            if categorized_token_file_path.exists(): 
                categorized_token_file_path.unlink()
            
            raw_fn_for_empty = f"{RAW_TOKEN_PREFIX}{report_hour_id_str}.json"
            if progress_data.get(raw_fn_for_empty, {}).get("fully_categorized"): 
                raw_file_to_delete = REPORTS_DIR / raw_fn_for_empty
                if raw_file_to_delete.exists():
                    try:
                        raw_file_to_delete.unlink(missing_ok=True)
                        if raw_fn_for_empty in progress_data:
                            del progress_data[raw_fn_for_empty] 
                            save_categorization_progress(progress_data)
                    except Exception as e_del_empty_raw:
                        print(f"[{get_log_ts()}] ⚠️ Mindshare Gen: Error deleting raw file for empty hour {raw_fn_for_empty}: {e_del_empty_raw}")
            return True 
        except Exception as e_save_empty_report:
            print(f"[{get_log_ts()}] 💥 Mindshare Gen: Error saving minimal report or deleting files for {report_hour_id_str}: {e_save_empty_report}")
            return False
    
    unclear_categories_to_refine = ["Other Meme", "Unclear/Generic", "Uncategorized by GPT", 
                                    "Error: GPT Category Call Failed", "Abstract", 
                                    "Error: Daily Rate Limit Hit", "Error: Max Retries Rate Limit", 
                                    "Error: GPT Batch Call Failed", "Error: Batch Gather Exception", 
                                    "Error: RPD Cooldown Active", "Error: GPT Logic Fallthrough",
                                    "Error: Max Retries RPM/TPM", "Error: Unexpected Batch Result", 
                                    "Uncategorized (Invalid Raw Data)", "Error: GPT Batch Output Mismatch",
                                    "Error: GPT Batch Non-JSON Response", "Error: GPT Batch All Retries Failed",
                                    "Uncategorized by GPT (Batch Empty)"] 
                                    
    tokens_needing_refinement = [
        token for token in all_categorized_tokens_this_hour 
        if token.get("gpt_initial_category") in unclear_categories_to_refine
    ]

    if len(tokens_needing_refinement) >= MIN_TOKENS_FOR_UNCLEAR_REANALYSIS:
        unique_unclear_tokens_for_gpt = []
        seen_mints_for_refinement = set() 
        for token in tokens_needing_refinement:
            mint = token.get("mint")
            unique_key = mint if mint else f"{token.get('name', 'N/A')}-{token.get('symbol', 'N/A')}-{random.random()}" 
            
            if unique_key not in seen_mints_for_refinement:
                unique_unclear_tokens_for_gpt.append(token) 
                seen_mints_for_refinement.add(unique_key)
        
        if unique_unclear_tokens_for_gpt: 
            refined_category_map = gpt_refine_unclear_categories(unique_unclear_tokens_for_gpt) 
            
            if refined_category_map and "error" not in refined_category_map:
                updated_count = 0
                for new_theme, mint_list_from_gpt in refined_category_map.items():
                    if new_theme == "still_unclear_or_unique_mints" or not isinstance(mint_list_from_gpt, list):
                        continue
                    for token_mint_to_update in mint_list_from_gpt: 
                        for token_in_main_list in all_categorized_tokens_this_hour:
                            if token_in_main_list.get("mint") == token_mint_to_update:
                                token_in_main_list["gpt_initial_category"] = new_theme 
                                token_in_main_list["refined_category"] = True 
                                updated_count +=1
                                break 
                if updated_count > 0:
                    print(f"[{get_log_ts()}] ✅ Mindshare Gen: Refined categories for {updated_count} tokens in hour {report_hour_id_str} (UTC).")
    
    try: report_hour_dt_utc_naive = datetime.strptime(report_hour_id_str, '%Y-%m-%d_%H')
    except ValueError: 
        print(f"[{log_ts}] ⚠️ Mindshare Gen: Invalid hour format in report_hour_id_str '{report_hour_id_str}'. Using approximation for migration window.")
        report_hour_dt_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1) 
        
    all_migration_events_in_window = load_migrated_mints_for_window(report_hour_dt_utc_naive, window_hours=3)
    migrated_mints_this_window_set = {event['mint'] for event in all_migration_events_in_window if 'mint' in event}

    bonded_token_analyses = []
    valid_tokens_for_bond_analysis = [
        t for t in all_categorized_tokens_this_hour 
        if t.get("mint") and t.get("gpt_initial_category") and not any(err_str in t["gpt_initial_category"] for err_str in ["Error:", "Uncategorized (Invalid", "Uncategorized by GPT"])
    ]

    for categorized_token_data in valid_tokens_for_bond_analysis: 
        mint = categorized_token_data.get("mint") 
        if mint in migrated_mints_this_window_set:
            gpt_target_info = categorized_token_data 
            gpt_result_for_bonded = gpt_get_theme_and_related_pre_bond_from_categorized( 
                gpt_target_info, valid_tokens_for_bond_analysis 
            )
            migration_event_data = next((m_event for m_event in all_migration_events_in_window if m_event.get("mint") == mint), {})
            analysis_entry = {
                "bonded_token_mint": mint,
                "bonded_token_name": categorized_token_data.get("name"),
                "bonded_token_symbol": categorized_token_data.get("symbol"),
                "bonded_token_final_category": categorized_token_data.get("gpt_initial_category"), 
                "creation_timestamp_utc": categorized_token_data.get("timestamp_utc"),
                "migration_event_data_snippet": {k: migration_event_data.get(k) for k in ["migration_timestamp_utc", "tx_type_if_present"] if k in migration_event_data},
                "gpt_narrative_link_analysis": gpt_result_for_bonded
            }
            bonded_token_analyses.append(analysis_entry)
            time.sleep(1.0) 

    valid_tokens_for_report_stats = [
        t for t in all_categorized_tokens_this_hour 
        if t.get("gpt_initial_category") and not any(err_str in t["gpt_initial_category"] for err_str in ["Error:", "Uncategorized (Invalid", "Uncategorized by GPT"])
    ]
    overall_pre_bond_category_counts = Counter(t.get("gpt_initial_category", "Uncategorized by GPT") for t in valid_tokens_for_report_stats) 
    
    bonded_theme_strength_scores = Counter()
    for analysis in bonded_token_analyses: 
        if analysis.get("gpt_narrative_link_analysis") and "error" not in analysis["gpt_narrative_link_analysis"]:
            theme = analysis["gpt_narrative_link_analysis"].get("final_target_token_theme_confirmation", 
                                                               analysis.get("bonded_token_final_category", "Uncategorized"))
            score = analysis["gpt_narrative_link_analysis"].get("count_related_pre_bond_by_gpt", 0) + 1 
            bonded_theme_strength_scores[theme] += score
    dominant_bonded_narratives = dict(bonded_theme_strength_scores.most_common(5))

    potential_outliers = [] 
    if len(bonded_token_analyses) > 1 and len(valid_tokens_for_report_stats) > 0:
        for analysis in bonded_token_analyses:
            bonded_cat = analysis.get("bonded_token_final_category", "Uncategorized")
            is_dominant_theme = any(dom_theme.lower() == bonded_cat.lower() for dom_theme in dominant_bonded_narratives.keys())
            category_overall_prebond_count = overall_pre_bond_category_counts.get(bonded_cat, 0)
            if not is_dominant_theme and category_overall_prebond_count < max(2, len(valid_tokens_for_report_stats) * 0.01): 
                 potential_outliers.append({
                     "token": f"{analysis.get('bonded_token_symbol','???')} - {analysis.get('bonded_token_name','UnkName')}",
                     "mint": analysis.get("bonded_token_mint"),
                     "category": bonded_cat,
                     "reason": f"Category '{bonded_cat}' not dominant among bonded and had low pre-bond presence ({category_overall_prebond_count} instances)."
                 })

    alert_parts = [f"Mindshare Report for Hour {report_hour_id_str} (UTC):"] 
    alert_parts.append(f"{len(valid_tokens_for_report_stats)} valid pre-bond tokens analyzed (Total in categorized file: {len(all_categorized_tokens_this_hour)}).")
    alert_parts.append(f"{len(bonded_token_analyses)} token(s) created this hour (UTC) also migrated & were analyzed for narrative links.")
    if dominant_bonded_narratives:
        alert_parts.append("Strongest narratives (by bonded tokens & thematic pre-bond support): " + 
                           ", ".join([f"{theme} (score: {score})" for theme, score in dominant_bonded_narratives.items()]))
    top_prebond_cats = dict(overall_pre_bond_category_counts.most_common(3))
    if top_prebond_cats:
         alert_parts.append(f"Top overall pre-bond categories this hour (after refinement): " +
                            ", ".join([f"{cat} ({cnt})" for cat, cnt in top_prebond_cats.items()]))
    if potential_outliers:
        alert_parts.append(f"Potential Outliers among Bonded ({len(potential_outliers)}): " + 
                           "; ".join([f"{o['token']} (Cat: {o['category']})" for o in potential_outliers[:2]])) 

    hourly_mindshare_alert = " ".join(alert_parts)
    print(f"[{get_log_ts()}] HOURLY MINDSHARE ALERT (Hour {report_hour_id_str} UTC): {hourly_mindshare_alert}")

    final_report_data = {
        "report_generation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "target_creation_hour_id": report_hour_id_str, 
        "total_tokens_in_categorized_file": len(all_categorized_tokens_this_hour),
        "total_valid_tokens_analyzed": len(valid_tokens_for_report_stats),
        "pre_bond_category_distribution_after_refinement": dict(overall_pre_bond_category_counts),
        "total_bonded_tokens_from_this_hour_analyzed": len(bonded_token_analyses),
        "dominant_bonded_narratives_with_strength": dominant_bonded_narratives,
        "potential_outliers_among_bonded": potential_outliers,
        "hourly_mindshare_alert": hourly_mindshare_alert,
        "detailed_bonded_token_analyses": bonded_token_analyses
    }

    report_filename = REPORTS_DIR / f"{MINDSHARE_REPORT_PREFIX}{report_hour_id_str}.json"
    try:
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(final_report_data, f, ensure_ascii=False, indent=2)
        print(f"[{get_log_ts()}] 📝 Mindshare Gen: Mindshare report saved: {report_filename.name}")
        
        if categorized_token_file_path.exists(): 
            categorized_token_file_path.unlink() 
        
        raw_filename_to_check = f"{RAW_TOKEN_PREFIX}{report_hour_id_str}.json"
        if progress_data.get(raw_filename_to_check, {}).get("fully_categorized", False): 
            raw_file_to_delete = REPORTS_DIR / raw_filename_to_check
            if raw_file_to_delete.exists():
                try: 
                    raw_file_to_delete.unlink()
                    if raw_filename_to_check in progress_data:
                        del progress_data[raw_filename_to_check] 
                        save_categorization_progress(progress_data)
                except Exception as e_del_raw:
                     print(f"[{get_log_ts()}] ⚠️ Mindshare Gen: Error deleting fully categorized raw file {raw_filename_to_check}: {e_del_raw}")
        return True 
    except Exception as e_save_report:
        print(f"[{get_log_ts()}] 💥 Mindshare Gen: Error saving mindshare report or deleting files for {report_hour_id_str}: {e_save_report}")
        return False
    
# --- Main Loop (and new task functions) ---

async def categorization_worker(progress_data: dict):
    """Continuously categorizes pre-bond tokens."""
    log_ts = get_log_ts()
    print(f"[{log_ts}] Categorization Worker: Started.")
    last_raw_file_processed_for_context_tracker = None
    while True:
        try:
            _, last_raw_file_processed_for_context_tracker = \
                await continuously_categorize_pre_bond_tokens_async(progress_data, last_raw_file_processed_for_context_tracker)
            await asyncio.sleep(MAIN_LOOP_SLEEP_SECONDS)
        except Exception as e:
            print(f"[{get_log_ts()}] 💥 ERROR in Categorization Worker: {e}")
            traceback.print_exc()
            await asyncio.sleep(MAIN_LOOP_SLEEP_SECONDS * 3) 

async def mindshare_report_worker(progress_data: dict):
    """Periodically checks for and generates mindshare reports."""
    log_ts_init = get_log_ts() 
    print(f"[{log_ts_init}] Mindshare Report Worker: Started.")
    last_no_files_log_hour_marker = None
    
    while True:
        try:
            log_ts_cycle = get_log_ts()
            print(f"[{log_ts_cycle}] Mindshare Worker: === Checking for reports to generate ===")
            
            current_decision_time: datetime
            if LOCAL_TZ:
                current_decision_time = datetime.now(LOCAL_TZ)
                print(f"[{log_ts_cycle}] Mindshare Worker: Current time for checks (local {LOCAL_TZ.key}): {current_decision_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
            else:
                current_decision_time = datetime.now(timezone.utc) # Default to UTC aware if LOCAL_TZ is not set
                print(f"[{log_ts_cycle}] Mindshare Worker: Current time for checks (UTC, LOCAL_TZ not set): {current_decision_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

            pending_categorized_files = sorted(
                [f for f in REPORTS_DIR.iterdir() if f.name.startswith(CATEGORIZED_TOKEN_PREFIX) and f.is_file()],
                key=lambda x: x.name
            )
            reports_generated_this_cycle = 0

            if not pending_categorized_files:
                current_hour_marker_for_log = (datetime.now(LOCAL_TZ if LOCAL_TZ else timezone.utc)).strftime('%Y-%m-%d_%H')
                if last_no_files_log_hour_marker != current_hour_marker_for_log:
                    print(f"[{log_ts_cycle}] Mindshare Worker: No categorized files found to consider for Reports this cycle.")
                    last_no_files_log_hour_marker = current_hour_marker_for_log
            else:
                print(f"[{log_ts_cycle}] Mindshare Worker: Found {len(pending_categorized_files)} pending categorized files: {[f.name for f in pending_categorized_files]}")
                last_no_files_log_hour_marker = None 

            for cat_file_path in pending_categorized_files:
                file_hour_id_str = cat_file_path.stem.replace(CATEGORIZED_TOKEN_PREFIX, "")
                log_ts_file_check = get_log_ts() 
                
                try:
                    dt_from_filename_components = datetime.strptime(file_hour_id_str, '%Y-%m-%d_%H')
                except ValueError:
                    print(f"[{log_ts_file_check}] Mindshare Worker: Invalid hour format in {cat_file_path.name} ({file_hour_id_str}). Skipping.")
                    continue

                process_file = False
                
                if LOCAL_TZ:
                    cutoff_base_this_file_local_aware = datetime(
                        dt_from_filename_components.year,
                        dt_from_filename_components.month,
                        dt_from_filename_components.day,
                        dt_from_filename_components.hour, 
                        0, 0, tzinfo=LOCAL_TZ
                    )
                    target_cutoff_this_file_local_aware = cutoff_base_this_file_local_aware + \
                                                         timedelta(hours=1) + \
                                                         timedelta(minutes=MINUTES_PAST_HOUR_FOR_MINDSHARE_CUTOFF)
                    
                    print(f"[{log_ts_file_check}] Mindshare Worker: Evaluating {cat_file_path.name} (UTC hour in filename: {dt_from_filename_components.hour})")
                    print(f"[{log_ts_file_check}] Mindshare Worker: Using local time interpretation for cutoff. Filename hour {dt_from_filename_components.hour} treated as local for cutoff base.")
                    print(f"[{log_ts_file_check}] Mindshare Worker: -> Target cutoff (local {LOCAL_TZ.key}): {target_cutoff_this_file_local_aware.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

                    if current_decision_time >= target_cutoff_this_file_local_aware:
                        process_file = True
                    else:
                        print(f"[{log_ts_file_check}] Mindshare Worker: -> Skipping {cat_file_path.name} - too early. (Current local time {current_decision_time.strftime('%H:%M:%S %Z%z')} < Cutoff {target_cutoff_this_file_local_aware.strftime('%H:%M:%S %Z%z')})")
                
                else: 
                    print(f"[{log_ts_file_check}] Mindshare Worker: LOCAL_TZ not set. Using UTC-based cutoff for {cat_file_path.name} (UTC hour in filename: {dt_from_filename_components.hour})")
                    current_processing_time_utc_naive = current_decision_time.replace(tzinfo=None)
                    end_of_cat_file_hour_utc_naive = dt_from_filename_components + timedelta(hours=1)
                    cutoff_time_for_report_utc_naive = end_of_cat_file_hour_utc_naive + timedelta(minutes=MINUTES_PAST_HOUR_FOR_MINDSHARE_CUTOFF)
                    
                    print(f"[{log_ts_file_check}] Mindshare Worker: -> File's UTC hour ends {end_of_cat_file_hour_utc_naive.strftime('%Y-%m-%d %H:%M:%S')} UTC, report cutoff {cutoff_time_for_report_utc_naive.strftime('%Y-%m-%d %H:%M:%S')} UTC")

                    if current_processing_time_utc_naive >= cutoff_time_for_report_utc_naive:
                        process_file = True
                    else:
                        print(f"[{log_ts_file_check}] Mindshare Worker: -> Skipping {cat_file_path.name} - too early. (Current UTC naive time {current_processing_time_utc_naive.strftime('%H:%M:%S')} < Cutoff {cutoff_time_for_report_utc_naive.strftime('%H:%M:%S')})")

                if not process_file:
                    continue 
                
                # Common checks after cutoff decision
                mindshare_report_file = REPORTS_DIR / f"{MINDSHARE_REPORT_PREFIX}{file_hour_id_str}.json"
                raw_filename_for_check = f"{RAW_TOKEN_PREFIX}{file_hour_id_str}.json" # Used for RPD check and progress data

                if mindshare_report_file.exists(): 
                    print(f"[{log_ts_file_check}] Mindshare Worker: Report for {file_hour_id_str} already exists ({mindshare_report_file.name}).")
                    if cat_file_path.exists(): 
                        try: 
                            print(f"[{log_ts_file_check}] Mindshare Worker: Cleaning up categorized file {cat_file_path.name}.")
                            cat_file_path.unlink()
                        except OSError as e_unlink_cat: print(f"[{get_log_ts()}] Mindshare Worker: Error unlinking {cat_file_path.name}: {e_unlink_cat}")
                    
                    # Raw file cleanup (if applicable) is handled inside generate_hourly_mindshare_report_from_categorized
                    # or if the report already exists and the raw file is marked fully_categorized.
                    raw_file_to_delete_if_reported = REPORTS_DIR / raw_filename_for_check
                    if progress_data.get(raw_filename_for_check, {}).get("fully_categorized", False) and raw_file_to_delete_if_reported.exists():
                        try:
                            print(f"[{log_ts_file_check}] Mindshare Worker: Cleaning up fully categorized raw file {raw_filename_for_check} as report exists.")
                            raw_file_to_delete_if_reported.unlink()
                            if raw_filename_for_check in progress_data: 
                                del progress_data[raw_filename_for_check]
                                save_categorization_progress(progress_data) 
                        except Exception as e_unlink_raw:  print(f"[{get_log_ts()}] Mindshare Worker: Error unlinking raw file {raw_filename_for_check}: {e_unlink_raw}")
                    continue 

                # Removed: is_fully_categorized_by_progress check for report generation eligibility.
                # The decision to proceed is now based on time cutoff and categorized file existence.
                
                if raw_filename_for_check in rpd_hit_timestamps and \
                   time.time() < rpd_hit_timestamps[raw_filename_for_check] + RPD_HIT_COOLDOWN_SECONDS:
                    print(f"[{log_ts_file_check}] Mindshare Worker: Skipping {cat_file_path.name}, raw file {raw_filename_for_check} under RPD cooldown.")
                    continue 
                
                if not cat_file_path.exists(): 
                    print(f"[{log_ts_file_check}] Mindshare Worker: Categorized file {cat_file_path.name} disappeared before processing. Skipping.")
                    continue

                print(f"[{get_log_ts()}] >> Mindshare Worker: Processing fully categorized file for Mindshare Report: {cat_file_path.name}")
                
                loop = asyncio.get_event_loop()
                report_generated_successfully = await loop.run_in_executor(
                    None, 
                    generate_hourly_mindshare_report_from_categorized, 
                    cat_file_path, 
                    progress_data
                )
                
                if report_generated_successfully: 
                    reports_generated_this_cycle += 1
                else: 
                    print(f"[{get_log_ts()}] Mindshare Worker: Failed to generate report for {cat_file_path.name} (returned False).")
            
            if reports_generated_this_cycle > 0:
                print(f"[{get_log_ts()}] ✅ Mindshare Worker: Generated {reports_generated_this_cycle} Mindshare Report(s) this cycle.")
            
            print(f"[{get_log_ts()}] Mindshare Worker: === Finished checking for reports. Sleeping for {MINDSHARE_REPORT_INTERVAL_SECONDS}s. ===")
            await asyncio.sleep(MINDSHARE_REPORT_INTERVAL_SECONDS)

        except Exception as e:
            print(f"[{get_log_ts()}] 💥 ERROR in Mindshare Report Worker: {e}")
            traceback.print_exc()
            await asyncio.sleep(MINDSHARE_REPORT_INTERVAL_SECONDS) 


async def main_analysis_loop():
    print(f"[{get_log_ts()}] --- Continuous Narrative Analyzer V7.3.9 (Removed fully_categorized check for Mindshare eligibility) ---") 
    global rpd_hit_timestamps 

    categorization_progress = load_categorization_progress()

    cat_task = asyncio.create_task(categorization_worker(categorization_progress))
    mindshare_task = asyncio.create_task(mindshare_report_worker(categorization_progress))

    try:
        await asyncio.gather(cat_task, mindshare_task)
    except Exception as e_gather:
        print(f"[{get_log_ts()}] 💥 CRITICAL ERROR in asyncio.gather: {e_gather}")
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main_analysis_loop())
    except KeyboardInterrupt:
        print(f"\n[{get_log_ts()}] 🛑 Analyzer stopped by user (main entry).")
    except Exception as e_main:
        print(f"[{get_log_ts()}] 💥 CRITICAL UNHANDLED ERROR in __main__: {e_main}")
        traceback.print_exc()
    finally:
        print(f"[{get_log_ts()}] --- Continuous Narrative Analyzer V7.3.9 Main Process Exited ---")
