import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter, defaultdict

# Configuration
REPORTS_DIR = Path(r"C:\Users\joaob\Desktop\solana-tracker\narrative_reports")
FINDINGS_PREFIX = "findings_"
DAILY_SUMMARY_PREFIX = "daily_summary_"
LOG_TS_FORMAT = '%Y-%m-%d %H:%M:%S'

# Helper for formatted log timestamps
def get_log_ts():
    return datetime.now().strftime(LOG_TS_FORMAT)

def create_daily_summary(target_date: datetime):
    """
    Generates a daily summary from all findings_YYYY-MM-DD_HH.json files for the target_date.
    Deletes the hourly findings files after successful summary creation.
    """
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"[{get_log_ts()}] 🔄 Starting daily summary generation for {date_str}...")

    hourly_findings_files_to_process = []
    for hour in range(24): # 00 to 23
        current_hour_dt = target_date.replace(hour=hour)
        findings_filename = REPORTS_DIR / f"{FINDINGS_PREFIX}{current_hour_dt.strftime('%Y-%m-%d_%H')}.json"
        if findings_filename.exists():
            hourly_findings_files_to_process.append(findings_filename)

    if not hourly_findings_files_to_process:
        print(f"[{get_log_ts()}] ⚠️ No hourly findings files found for {date_str}. No summary will be generated.")
        return

    print(f"[{get_log_ts()}] 🔍 Found {len(hourly_findings_files_to_process)} hourly findings files for {date_str} to aggregate.")

    all_narratives_count = Counter()
    repeated_token_signatures = Counter() # Stores "SYMBOL - NAME"
    tokens_by_hour_count = [0] * 24
    total_bonded_tokens_day = 0
    
    processed_source_raw_files = set() # Track raw files mentioned in findings

    for findings_file_path in hourly_findings_files_to_process:
        print(f"[{get_log_ts()}] 📄 Processing findings file: {findings_file_path.name}")
        try:
            with open(findings_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            raw_file_name = data.get("source_raw_file_name")
            if raw_file_name:
                processed_source_raw_files.add(raw_file_name)
            
            bonded_count_in_file = data.get("bonded_tokens_count", 0)
            total_bonded_tokens_day += bonded_count_in_file
            
            file_hour_id_str = data.get("source_file_hour_id", findings_file_path.stem.replace(FINDINGS_PREFIX, ""))
            try:
                file_hour = int(file_hour_id_str.split('_')[1])
                if 0 <= file_hour <= 23:
                    tokens_by_hour_count[file_hour] += bonded_count_in_file
            except (IndexError, ValueError, TypeError) as e:
                print(f"[{get_log_ts()}] ⚠️ Could not parse hour from '{file_hour_id_str}' in file {findings_file_path.name}: {e}")

            categorized_data = data.get("categorized_bonded_tokens", {})
            if isinstance(categorized_data, dict):
                for category, tokens_in_cat_list in categorized_data.items():
                    if isinstance(tokens_in_cat_list, list):
                        all_narratives_count[category] += len(tokens_in_cat_list)
                        for token_str in tokens_in_cat_list: # token_str is "SYMBOL - NAME"
                            repeated_token_signatures[token_str] += 1
            
        except json.JSONDecodeError:
            print(f"[{get_log_ts()}] ⚠️ Error decoding JSON from {findings_file_path.name}. Skipping this file.")
        except Exception as e:
            print(f"[{get_log_ts()}] 💥 Unexpected error processing {findings_file_path.name}: {e}")

    # Filter for names/symbols that appeared more than once
    actual_repeated_tokens = {name: count for name, count in repeated_token_signatures.items() if count > 1}

    # Find peak minting hours
    max_token_count_in_hour = 0
    if any(tokens_by_hour_count): # check if there are any tokens at all
        max_token_count_in_hour = max(tokens_by_hour_count)
    
    peak_hours_list = [
        f"{h:02d}:00-{h:02d}:59" for h, count in enumerate(tokens_by_hour_count) 
        if count == max_token_count_in_hour and max_token_count_in_hour > 0
    ]

    summary_data = {
        "summary_generation_timestamp": datetime.now().isoformat(),
        "date_summarized": date_str,
        "total_hourly_findings_processed": len(hourly_findings_files_to_process),
        "associated_raw_token_files": sorted(list(processed_source_raw_files)),
        "total_bonded_tokens_for_day": total_bonded_tokens_day,
        "narrative_popularity_by_token_count": dict(all_narratives_count.most_common()),
        "repeated_token_signatures_counts": dict(sorted(actual_repeated_tokens.items(), key=lambda item: item[1], reverse=True)),
        "bonded_tokens_by_hour_of_day": {f"{h:02d}:00-{h:02d}:59": tokens_by_hour_count[h] for h in range(24)},
        "peak_minting_hours_by_bonded_tokens": peak_hours_list
    }

    daily_summary_filename = REPORTS_DIR / f"{DAILY_SUMMARY_PREFIX}{date_str}.json"
    try:
        with open(daily_summary_filename, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, ensure_ascii=False, indent=2)
        print(f"[{get_log_ts()}] ✅ Daily summary saved to: {daily_summary_filename.name}")

        # Delete hourly findings files after successful summary creation
        deleted_count = 0
        for findings_file_path_to_delete in hourly_findings_files_to_process:
            try:
                findings_file_path_to_delete.unlink()
                deleted_count +=1
            except OSError as e:
                print(f"[{get_log_ts()}] ⚠️ Error deleting hourly findings file {findings_file_path_to_delete.name}: {e}")
        print(f"[{get_log_ts()}] ✅ Deleted {deleted_count}/{len(hourly_findings_files_to_process)} hourly findings files for {date_str}.")

    except OSError as e:
        print(f"[{get_log_ts()}] ⚠️ Error writing daily summary file {daily_summary_filename.name}: {e}")
    except Exception as e:
        print(f"[{get_log_ts()}] 💥 Unexpected error during daily summary save or findings deletion for {date_str}: {e}")

if __name__ == "__main__":
    print(f"[{get_log_ts()}] --- Daily Summary Generator Started ---")
    
    # Summarize for "yesterday"
    # This script should be run once per day, after all hourly analyses for the previous day are complete.
    # e.g., run at 00:15 AM or 01:05 AM to summarize the entire previous day.
    yesterday_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    create_daily_summary(target_date=yesterday_date)
    
    print(f"[{get_log_ts()}] --- Daily Summary Generator Finished ---")