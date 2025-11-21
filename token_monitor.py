import websocket
import json
import time
import threading
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("HELIUS_API_KEY")
if not API_KEY:
    raise ValueError("HELIUS_API_KEY not found in .env file. Please create a .env file with your API key.")

WS_URL = f"wss://atlas-mainnet.helius-rpc.com/?api-key={API_KEY}"
JSON_OUTPUT_FILE = "new_tokens.json"

# Solana Program IDs
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
METAPLEX_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s" # Metaplex Token Metadata Program

# List of token programs to monitor
TOKEN_PROGRAM_IDS_TO_MONITOR = [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]

# --- Global State ---
discovered_tokens = []
processed_mints = set() # To avoid processing the same mint multiple times from different related logs

# --- Helper Functions ---
def save_tokens_to_json(tokens, filename=JSON_OUTPUT_FILE):
    """Saves the list of discovered tokens to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(tokens, f, indent=4)
        print(f"Successfully saved {len(tokens)} tokens to {filename}")
    except Exception as e:
        print(f"Error saving tokens to JSON: {e}")

def load_tokens_from_json(filename=JSON_OUTPUT_FILE):
    """Loads tokens from a JSON file to resume and avoid duplicates."""
    global discovered_tokens, processed_mints
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                discovered_tokens = json.load(f)
                for token in discovered_tokens:
                    if "mint" in token:
                        processed_mints.add(token["mint"])
                print(f"Loaded {len(discovered_tokens)} existing tokens from {filename}. Processed mints: {len(processed_mints)}")
        except Exception as e:
            print(f"Error loading tokens from JSON: {e}, starting fresh.")
            discovered_tokens = []
            processed_mints = set()
    else:
        print(f"{filename} not found, starting with an empty token list.")

# --- WebSocket Event Handlers ---
def on_message(ws, message_str):
    global discovered_tokens, processed_mints
    try:
        data = json.loads(message_str)

        # Check for subscription confirmation
        if "result" in data and isinstance(data["result"], int) and "id" in data:
            print(f"Subscription successful with ID: {data['result']}")
            return

        # Ensure it's a transaction notification
        if "params" not in data or "result" not in data["params"] or "transaction" not in data["params"]["result"]:
            # print(f"Non-transaction message or unexpected format: {json.dumps(data, indent=2)}")
            return

        transaction_notification = data["params"]["result"]
        transaction_data = transaction_notification.get("transaction")
        
        if not transaction_data:
            print("No transaction data in notification.")
            return

        # Get transaction signature for logging
        signature = transaction_data.get("signatures", [None])[0]
        if not signature:
            print("Transaction has no signature.")
            return
        
        # print(f"\n--- Processing Transaction: {signature} ---")
        # uncomment above for very verbose logging of every transaction

        instructions = transaction_data.get("message", {}).get("instructions", [])
        account_keys_list = transaction_data.get("message", {}).get("accountKeys", []) # List of account objects or strings
        
        # Ensure account_keys are strings if they are objects with 'pubkey'
        account_keys = []
        for acc in account_keys_list:
            if isinstance(acc, dict) and "pubkey" in acc:
                account_keys.append(acc["pubkey"])
            elif isinstance(acc, str):
                account_keys.append(acc)


        # Temporary holders for data from this transaction
        mint_address_found = None
        decimals_found = None
        token_name_found = None
        token_symbol_found = None
        token_uri_found = None
        metaplex_instruction_mint = None # Mint associated with metaplex instruction

        for ix in instructions:
            program_id = ix.get("programId")
            parsed_ix = ix.get("parsed")
            if not parsed_ix:
                continue

            ix_type = parsed_ix.get("type")
            info = parsed_ix.get("info")

            # 1. Look for InitializeMint or InitializeMint2 instruction
            if program_id in TOKEN_PROGRAM_IDS_TO_MONITOR and ix_type in ["initializeMint", "initializeMint2"] and info:
                mint_address_found = info.get("mint")
                decimals_found = info.get("decimals")
                # print(f"  [{signature[:10]}...] Found InitializeMint: Mint={mint_address_found}, Decimals={decimals_found}")

            # 2. Look for Metaplex CreateMetadataAccount instruction
            #    Common types: createMetadataAccountV3, createMetadataAccount (older)
            elif program_id == METAPLEX_PROGRAM_ID and ix_type in ["createMetadataAccountV3", "createMetadataAccount"] and info:
                # Helius's parsed output for 'createMetadataAccountV3' should have 'mint' in info
                metaplex_instruction_mint = info.get("mint")

                # Extract metadata (name, symbol, uri)
                # For V3, 'data' is often directly in 'info' or nested.
                # For older versions, it's typically in 'info.data'.
                metadata_container = info.get("data", info) # Try info.data, fallback to info
                
                token_name_found = metadata_container.get("name")
                token_symbol_found = metadata_container.get("symbol")
                token_uri_found = metadata_container.get("uri")

                # print(f"  [{signature[:10]}...] Found Metaplex {ix_type}: Name={token_name_found}, Symbol={token_symbol_found}, URI={token_uri_found}, For Mint={metaplex_instruction_mint}")
        
        # 3. Correlate and Store if a new token is fully identified
        if mint_address_found and token_name_found and token_symbol_found and (metaplex_instruction_mint == mint_address_found):
            if mint_address_found not in processed_mints:
                # Clean null characters that sometimes appear in on-chain string data
                cleaned_name = token_name_found.replace('\x00', '').strip()
                cleaned_symbol = token_symbol_found.replace('\x00', '').strip()
                cleaned_uri = token_uri_found.replace('\x00', '').strip() if token_uri_found else None

                token_data = {
                    "mint": mint_address_found,
                    "name": cleaned_name,
                    "symbol": cleaned_symbol,
                    "uri": cleaned_uri,
                    "decimals": decimals_found,
                    "transaction_signature": signature,
                    "timestamp_discovered": time.time() 
                }
                discovered_tokens.append(token_data)
                processed_mints.add(mint_address_found)
                
                print(f"\n✅ NEW TOKEN DISCOVERED! ✅")
                print(f"  Name: {cleaned_name}")
                print(f"  Symbol: {cleaned_symbol}")
                print(f"  Mint: {mint_address_found}")
                print(f"  Decimals: {decimals_found}")
                print(f"  URI: {cleaned_uri}")
                print(f"  Transaction: https://solscan.io/tx/{signature}")
                print(f"  Total Discovered: {len(discovered_tokens)}\n")
                
                save_tokens_to_json(discovered_tokens) # Save after each new find
            else:
                # print(f"  [{signature[:10]}...] Token {mint_address_found} ({token_name_found}) already processed.")
                pass
        # elif mint_address_found:
            # print(f"  [{signature[:10]}...] Mint {mint_address_found} initialized, but no complete Metaplex metadata found in THIS transaction.")
            # pass # Only mint initialized, no metadata in this TX

    except json.JSONDecodeError:
        # print(f"Could not decode JSON: {message_str[:200]}...") # Log truncated message
        pass # Helius might send non-JSON pings or other control messages
    except Exception as e:
        print(f"Error processing message: {e}")
        import traceback
        traceback.print_exc()


def on_error(ws, error):
    print(f"--- WebSocket Error ---")
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("--- WebSocket Connection Closed ---")
    if close_status_code or close_msg:
        print(f"Status Code: {close_status_code}, Message: {close_msg}")
    # Attempt to save one last time
    print("Attempting final save on close...")
    save_tokens_to_json(discovered_tokens)

def on_open(ws):
    print("--- WebSocket Connection Established ---")
    print(f"Monitoring for new tokens from programs: {', '.join(TOKEN_PROGRAM_IDS_TO_MONITOR)}")
    
    # Subscription request for transactions involving the specified token programs
    request = {
        "jsonrpc": "2.0",
        "id": 1, # Can be any integer
        "method": "transactionSubscribe",
        "params": [
            {
                "mentions": TOKEN_PROGRAM_IDS_TO_MONITOR, # Helius specific: monitor if any of these accounts are mentioned
                # "accountInclude": TOKEN_PROGRAM_IDS_TO_MONITOR, # More generic: monitor if these accounts are writable in the tx
            },
            {
                "commitment": "processed", # "processed", "confirmed", or "finalized"
                "encoding": "jsonParsed", # Get parsed transaction data
                "transactionDetails": "full", # Need full details to parse instructions
                "maxSupportedTransactionVersion": 0 # For modern transactions
            }
        ]
    }
    ws.send(json.dumps(request))
    print("Subscription request sent.")
    
    # Start ping thread to keep connection alive (Helius might not require this if activity is high)
    def ping_thread_func():
        while True:
            try:
                # Helius websockets might not need explicit pings if there's steady data flow
                # If issues arise, uncomment the ping.
                # ws.ping()
                # print("Ping sent")
                time.sleep(30) # Send ping every 30 seconds
            except websocket.WebSocketConnectionClosedException:
                print("Ping thread: Connection closed, stopping pinger.")
                break
            except Exception as e:
                print(f"Ping thread error: {e}")
                break # Exit thread on other errors
    
    # No need for ping thread if Helius doesn't require it or handles it.
    # If you face disconnects, you can re-enable this.
    # ping_thread = threading.Thread(target=ping_thread_func, daemon=True)
    # ping_thread.start()
    # print("Ping thread started.")


# --- Main Execution ---
if __name__ == "__main__":
    load_tokens_from_json() # Load existing data

    print(f"Connecting to WebSocket: {WS_URL.split('?')[0]}...") # Hide API key from print

    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    try:
        ws_app.run_forever()
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Shutting down...")
    finally:
        print("Performing final save...")
        save_tokens_to_json(discovered_tokens)
        print("Shutdown complete.")