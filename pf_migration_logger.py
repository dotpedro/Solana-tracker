import asyncio
import websockets
import json
from datetime import datetime, timezone # For timezone-aware UTC
from pathlib import Path

# Config
SAVE_FOLDER = Path(r"C:\Users\joaob\Desktop\solana-tracker\narrative_reports") # Same save folder
LOG_TS_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
MIGRATED_TOKEN_PREFIX = "migrated_tokens_" # Dedicated prefix for these logs

# --- IMPORTANT: VERIFY THESE FROM YOUR WEB SOCKET TEST ---
# These are placeholders for how you might identify a migration event.
# You MUST confirm these against the actual WebSocket stream.
EXPECTED_MIGRATION_TX_TYPE = "migrate" # Example value for txType
# ALTERNATIVE_MIGRATION_METHOD_NAME = "tokenMigrated" # Example if method field is used

def get_log_ts():
    return datetime.now().strftime(LOG_TS_FORMAT)[:-3] # Local time for logs

def get_current_migration_log_filename():
    now = datetime.now() # Local time for filename generation
    return SAVE_FOLDER / f"{MIGRATED_TOKEN_PREFIX}{now.strftime('%Y-%m-%d_%H')}.json"

def append_migration_event_to_file(migration_event_data_to_save):
    target_filename = get_current_migration_log_filename()
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
                        print(f"[{log_ts}] (Migration Logger) ⚠️ Content of {target_filename.name} was not a list. Initializing new list.")
        except json.JSONDecodeError as je:
             print(f"[{log_ts}] (Migration Logger) ⚠️ Error parsing JSON from {target_filename.name} during append: {je}. Initializing new list.")
        except Exception as e: 
            print(f"[{log_ts}] (Migration Logger) ⚠️ Error reading {target_filename.name} during append: {e}. Initializing new list.")
    
    existing_data.append(migration_event_data_to_save)
    
    try:
        with open(target_filename, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print(f"[{log_ts}] (Migration Logger) 💾 Appended migration event to {target_filename.name}. Total events: {len(existing_data)}")
    except Exception as e:
        print(f"[{log_ts}] (Migration Logger) 💥 Error writing migration event to {target_filename.name}: {e}. Data: {migration_event_data_to_save}")


async def subscribe_and_log_migration_events():
    uri = "wss://pumpportal.fun/api/data"

    while True:
        try:
            print(f"[{get_log_ts()}] (Migration Logger) 📡 Connecting to WebSocket: {uri}...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                print(f"[{get_log_ts()}] (Migration Logger) ✅ Connected to WebSocket.")
                
                payload = {"method": "subscribeMigration"}
                await websocket.send(json.dumps(payload))
                print(f"[{get_log_ts()}] (Migration Logger) ✅ Sent subscription request: {payload}")

                while True:
                    message = await websocket.recv()
                    received_at_local = datetime.now()
                    received_at_utc = datetime.now(timezone.utc) 
                    log_ts_msg = received_at_local.strftime(LOG_TS_FORMAT)[:-3]
                    
                    print(f"[{log_ts_msg}] (Migration Logger) 🔄 Raw WS Message: {message}") # Now always logs raw message

                    try:
                        print(f"[{log_ts_msg}] (Migration Logger) Parsing message as JSON...")
                        data = json.loads(message)
                        if not isinstance(data, dict):
                            print(f"[{log_ts_msg}] (Migration Logger) ⚠️ Parsed message is not a dictionary: {type(data)}. Original: {message[:200]}")
                            continue
                        
                        print(f"[{log_ts_msg}] (Migration Logger) Successfully parsed JSON. Checking for migration event criteria...")

                        # --- IDENTIFICATION LOGIC FOR MIGRATION EVENT ---
                        # YOU MUST VERIFY AND ADJUST THIS SECTION BASED ON ACTUAL SERVER MESSAGES!
                        
                        is_migration_event = False
                        event_identifier_found = "None"

                        tx_type_from_data = data.get("txType")
                        # method_from_data = data.get("method") # If server uses 'method' to identify event type

                        if tx_type_from_data == EXPECTED_MIGRATION_TX_TYPE:
                            if "mint" in data:
                                is_migration_event = True
                                event_identifier_found = f"txType: {EXPECTED_MIGRATION_TX_TYPE}"
                            else:
                                print(f"[{log_ts_msg}] (Migration Logger) ⚠️ Event has txType '{EXPECTED_MIGRATION_TX_TYPE}' but missing 'mint'. Data: {data}")
                        
                        # Example for an alternative identifier:
                        # elif method_from_data == ALTERNATIVE_MIGRATION_METHOD_NAME:
                        #     if data.get("data", {}).get("mint"): # If mint is nested
                        #          is_migration_event = True
                        #          event_identifier_found = f"method: {ALTERNATIVE_MIGRATION_METHOD_NAME}"
                        #     else:
                        #          print(f"[{log_ts_msg}] (Migration Logger) ⚠️ Event has method '{ALTERNATIVE_MIGRATION_METHOD_NAME}' but missing 'mint'. Data: {data}")

                        elif "mint" in data and "message" not in data and not tx_type_from_data : 
                            # Fallback: If it has a 'mint', isn't a server status 'message', and had NO txType.
                            # This assumes raw token data might be sent directly for migrations on this stream.
                            is_migration_event = True
                            event_identifier_found = "Fallback (mint present, no txType/message)"
                            print(f"[{log_ts_msg}] (Migration Logger) ❔ Identified as potential migration via fallback (has mint, no txType/message). Mint: {data.get('mint')}")
                        
                        # --- END IDENTIFICATION LOGIC ---

                        if is_migration_event:
                            mint_migrated = data.get("mint") # or data.get("data",{}).get("mint") if nested
                            print(f"[{log_ts_msg}] (Migration Logger) ✅ Identified as MIGRATION EVENT via '{event_identifier_found}'. Mint: {mint_migrated}")
                            
                            event_to_save = {
                                "mint": mint_migrated,
                                "migration_timestamp_utc": received_at_utc.isoformat(),
                                "source_event_identifier": event_identifier_found,
                                # Add other potentially useful fields from the 'data' object:
                                "bonding_curve_key": data.get("bondingCurveKey"), 
                                "market_cap_sol_at_migration": data.get("marketCapSol"), 
                                "tx_type_if_present": tx_type_from_data, # Log the txType for reference
                                # "raw_event_data": data # Optional: Store the full raw event for deep debugging
                            }
                            append_migration_event_to_file(event_to_save)
                            print(f"[{log_ts_msg}] (Migration Logger) 🏁 Token Migration Logged: {mint_migrated}")
                        
                        elif "message" in data and "subscribed" in data["message"].lower():
                             print(f"[{log_ts_msg}] (Migration Logger) ℹ️ Server subscription confirmation: {data.get('message')}")
                        
                        else: # Message was parsed as JSON dict, but didn't match migration or known server messages
                            print(f"[{log_ts_msg}] (Migration Logger) ❔ Unhandled JSON message structure on migration stream. txType: '{tx_type_from_data}'. Keys: {list(data.keys())}. Snippet: {str(data)[:200]}")


                    except json.JSONDecodeError as je:
                        print(f"[{log_ts_msg}] (Migration Logger) ⚠️ Malformed JSON (could not parse): {je}. Message: {message[:200]}")
                    except Exception as e:
                        print(f"[{log_ts_msg}] (Migration Logger) 💥 Error processing received message: {e}. Original Message: {message[:200]}")

        except websockets.exceptions.ConnectionClosed as cc_err:
            print(f"[{get_log_ts()}] (Migration Logger) 🔌 WebSocket connection closed: {cc_err}. Reconnecting in 10s...")
        except Exception as e: 
            print(f"[{get_log_ts()}] (Migration Logger) 💥 Top-level WebSocket/Script error: {type(e).__name__} - {e}. Reconnecting in 10s...")
        
        await asyncio.sleep(10) # Wait before retrying connection

if __name__ == "__main__":
    try:
        SAVE_FOLDER.mkdir(parents=True, exist_ok=True)
        print(f"[{get_log_ts()}] (Migration Logger) 📂 Storing migration event data in: {SAVE_FOLDER.resolve()}")
    except OSError as e:
        print(f"[{get_log_ts()}] (Migration Logger) ❌ CRITICAL: Could not create save folder {SAVE_FOLDER}: {e}. Exiting.")
        exit(1) 

    print(f"[{get_log_ts()}] (Migration Logger) --- Migration Event Logger Started ---")
    try:
        asyncio.run(subscribe_and_log_migration_events())
    except KeyboardInterrupt:
        print(f"\n[{get_log_ts()}] (Migration Logger) ℹ️ Program interrupted by user.")
    finally:
        print(f"[{get_log_ts()}] (Migration Logger) 👋 Program exited.")