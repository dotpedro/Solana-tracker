import asyncio
import websockets
import json
from datetime import datetime, timezone
from pathlib import Path
import heapq # For efficiently getting N largest/smallest items, though sorting is fine for few files

# Config
SAVE_FOLDER = Path(r"C:\Users\joaob\Desktop\solana-tracker\narrative_reports")
LOG_TS_FORMAT = '%Y-%m-%d %H:%M:%S.%f' 
RAW_TOKEN_PREFIX = "raw_tokens_"
MAX_RAW_FILES_TO_KEEP = 5 # Keep the last 5 hourly raw_tokens files

# Global to track current hour string to detect changes for cleanup
CURRENT_HOUR_FILENAME_STEM_CHECK = None 

def get_log_ts():
    return datetime.now().strftime(LOG_TS_FORMAT)[:-3]

def get_current_raw_filename():
    now = datetime.now()
    return SAVE_FOLDER / f"{RAW_TOKEN_PREFIX}{now.strftime('%Y-%m-%d_%H')}.json"

def append_token_to_raw_file(token_data_to_save):
    target_filename = get_current_raw_filename()
    log_ts = get_log_ts()
    existing_data = []

    if target_filename.exists():
        try:
            with open(target_filename, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    loaded_json = json.loads(content)
                    if isinstance(loaded_json, list):
                        existing_data = loaded_json
                    else: 
                        print(f"[{log_ts}] ⚠️ Content of {target_filename.name} was not a list. Initializing new list.")
        except json.JSONDecodeError as je:
             print(f"[{log_ts}] ⚠️ Error parsing JSON from {target_filename.name}: {je}. Initializing new list.")
        except Exception as e: 
            print(f"[{log_ts}] ⚠️ Error reading {target_filename.name}: {e}. Initializing new list.")
    
    existing_data.append(token_data_to_save)
    
    try:
        with open(target_filename, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[{log_ts}] 💥 Error writing token to {target_filename.name}: {e}")

def cleanup_old_raw_files():
    log_ts = get_log_ts()
    print(f"[{log_ts}] 🧹 Checking for old raw token files to cleanup...")
    try:
        raw_files = []
        for f_path in SAVE_FOLDER.iterdir():
            if f_path.is_file() and f_path.name.startswith(RAW_TOKEN_PREFIX) and f_path.name.endswith(".json"):
                try:
                    # Extract timestamp string from filename e.g., "2023-10-27_10"
                    # from "raw_tokens_2023-10-27_10.json"
                    timestamp_str = f_path.stem.replace(RAW_TOKEN_PREFIX, "")
                    # Convert to datetime for sorting (though string sort mostly works here)
                    # file_dt = datetime.strptime(timestamp_str, '%Y-%m-%d_%H')
                    raw_files.append(f_path) # Store Path object
                except ValueError:
                    print(f"[{log_ts}] ⚠️ Could not parse timestamp from filename: {f_path.name}")
        
        # Sort files by name (which includes the timestamp, so it's chronological)
        raw_files.sort(key=lambda x: x.name)

        if len(raw_files) > MAX_RAW_FILES_TO_KEEP:
            files_to_delete_count = len(raw_files) - MAX_RAW_FILES_TO_KEEP
            print(f"[{log_ts}] 🗑️ Found {len(raw_files)} raw files. Need to delete {files_to_delete_count} oldest files to keep {MAX_RAW_FILES_TO_KEEP}.")
            
            for i in range(files_to_delete_count):
                file_to_delete = raw_files[i] # Oldest files are at the beginning of sorted list
                try:
                    file_to_delete.unlink()
                    print(f"[{log_ts}] ✅ Deleted old raw file: {file_to_delete.name}")
                except OSError as e_del:
                    print(f"[{log_ts}] ⚠️ Error deleting old raw file {file_to_delete.name}: {e_del}")
        else:
            print(f"[{log_ts}] ℹ️ Found {len(raw_files)} raw files. No cleanup needed (keeping up to {MAX_RAW_FILES_TO_KEEP}).")

    except Exception as e:
        print(f"[{log_ts}] 💥 Error during raw file cleanup: {e}")


async def subscribe_and_log_new_tokens():
    global CURRENT_HOUR_FILENAME_STEM_CHECK # To track hour changes for cleanup
    uri = "wss://pumpportal.fun/api/data"
    
    # Initialize CURRENT_HOUR_FILENAME_STEM_CHECK
    CURRENT_HOUR_FILENAME_STEM_CHECK = get_current_raw_filename().stem

    while True:
        try:
            print(f"[{get_log_ts()}] 📡 Connecting to WebSocket: {uri}...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                print(f"[{get_log_ts()}] ✅ Connected to WebSocket.")
                
                await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                print(f"[{get_log_ts()}] ✅ Subscribed to 'subscribeNewToken' events.")

                while True:
                    message = await websocket.recv()
                    received_at_local = datetime.now() 
                    received_at_utc = datetime.now(timezone.utc)
                    log_ts_msg = received_at_local.strftime(LOG_TS_FORMAT)[:-3]
                    
                    # Check if hour has changed to trigger cleanup
                    new_filename_stem = get_current_raw_filename().stem
                    if new_filename_stem != CURRENT_HOUR_FILENAME_STEM_CHECK:
                        print(f"[{log_ts_msg}] 🔔 Hour changed (new file stem: {new_filename_stem}). Triggering old file cleanup.")
                        cleanup_old_raw_files() # Perform cleanup
                        CURRENT_HOUR_FILENAME_STEM_CHECK = new_filename_stem
                    
                    try:
                        data = json.loads(message)
                        if not isinstance(data, dict):
                            continue
                        
                        tx_type_from_data = data.get("txType")
                        is_new_token = False
                        if tx_type_from_data == "create":
                            if "mint" in data and "name" in data and "symbol" in data:
                                is_new_token = True
                        elif tx_type_from_data is None and "mint" in data and "name" in data and "symbol" in data and "message" not in data :
                            is_new_token = True
                        
                        if is_new_token:
                            token_to_save = {
                                "name": data.get("name"),
                                "symbol": data.get("symbol"),
                                "mint": data.get("mint"),
                                "timestamp_utc": received_at_utc.isoformat(), 
                                "creator": data.get("traderPublicKey") 
                            }
                            append_token_to_raw_file(token_to_save)
                            print(f"[{log_ts_msg}] 🆕 New Token: {token_to_save.get('symbol')} ({token_to_save.get('name')}), Mint: {token_to_save.get('mint')}")
                        
                        elif "message" in data and "subscribed" in data["message"].lower():
                             print(f"[{log_ts_msg}] ℹ️ Server subscription message: {data.get('message')}")
                        
                    except json.JSONDecodeError:
                        pass 
                    except Exception as e:
                        print(f"[{log_ts_msg}] 💥 Error processing WS message: {e}. Message: {message[:200]}")

        except Exception as e: 
            print(f"[{get_log_ts()}] 💥 Top-level WS error: {type(e).__name__} - {e}. Reconnecting in 10s...")
        
        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        SAVE_FOLDER.mkdir(parents=True, exist_ok=True)
        print(f"[{get_log_ts()}] 📂 Storing new token data in: {SAVE_FOLDER.resolve()}")
        # Initial cleanup on startup in case script was down
        cleanup_old_raw_files() 
    except OSError as e:
        print(f"[{get_log_ts()}] ❌ CRITICAL: Could not create save folder {SAVE_FOLDER}: {e}. Exiting.")
        exit(1) 

    try:
        asyncio.run(subscribe_and_log_new_tokens())
    except KeyboardInterrupt:
        print(f"\n[{get_log_ts()}] ℹ️ Program interrupted by user.")
    finally:
        print(f"[{get_log_ts()}] 👋 Program exited.")