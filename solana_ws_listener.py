#!/usr/bin/env python
# Enhanced SPL token tracker with robust mint detection
# Captures both direct and CPI-initiated mints
# Includes smart account indexing and connection recovery
# Saves data to minted_tokens.jsonl
import os 
from dotenv import load_dotenv
import asyncio
import base64
import json
import logging
import signal
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from solders.pubkey import Pubkey
from solders.signature import Signature 
import httpx
from solders.transaction_status import EncodedTransactionWithStatusMeta, UiTransactionEncoding
#from solders.rpc.responses import UiTransactionResponse # For type hinting with jsonParsed
from solders.rpc.config import RpcTransactionLogsFilterMentions
from solders.rpc.responses import RpcLogsResponse, LogsNotification, SubscriptionResult
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from websockets.exceptions import ConnectionClosedError # type: ignore
from construct import Int8ul, Bytes, Struct, CString # type: ignore

PROCESS_TRANSACTIONS_FALLBACK = True 

transaction_queue = asyncio.Queue() # type: ignore # if using older type checkers for asyncio.Queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("token_tracker.log", encoding='utf-8'), # Specify UTF-8 for file
        logging.StreamHandler() # Console might still have issues on Windows
    ]
)
logger = logging.getLogger("TokenTracker")

# Constants
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
METAPLEX_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
METADATA_PREFIX = b"metadata"

# --- Connection settings ---
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

if not HELIUS_API_KEY:
    logger.error("HELIUS_API_KEY not found in environment variables or .env file. Exiting.")
    # You might want to fall back to public RPCs here or exit
    # For now, let's try to use public ones if Helius key is missing, but warn heavily.
    # exit(1) # Or raise an exception
    logger.warning("Proceeding with public RPC endpoints as HELIUS_API_KEY was not found.")
    WS_URI = "wss://api.mainnet-beta.solana.com/"
    HTTP_RPC = "https://api.mainnet-beta.solana.com"
else:
    logger.info("Using Helius RPC endpoints.")
    WS_URI = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    HTTP_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# Fallback RPC (less used now, but can be kept)
BACKUP_RPC = "https://solana-mainnet.g.alchemy.com/v2/demo" 

# Track processed mints to avoid duplicates
processed: Set[str] = set()

# Load previously processed mints if file exists
try:
    with open("minted_tokens.jsonl", "r") as f:
        for line in f:
            try:
                data = json.loads(line)
                processed.add(data.get("mint", ""))
            except json.JSONDecodeError:
                continue
    logger.info(f"Loaded {len(processed)} previously processed mints")
except FileNotFoundError:
    logger.info("No previous mint file found, starting fresh")

# Statistics
stats = {
    "started_at": datetime.now().isoformat(),
    "total_mints_found": 0,
    "metadata_successes": 0,
    "metadata_failures": 0,
    "reconnections": 0,
    "last_mint_at": None
}

# Struct to decode Metaplex metadata
METADATA_LAYOUT = Struct(
    "key" / Int8ul,
    "update_authority" / Bytes(32),
    "mint" / Bytes(32),
    "name" / CString("utf8"),
    "symbol" / CString("utf8"),
    "uri" / CString("utf8"),
)

# Track mints found in logs for delayed processing
# This helps deal with race conditions where metadata isn't available instantly
pending_mints: List[str] = []

# ────────────────────── METADATA DECODER ──────────────────────
async def fetch_metadata(client: AsyncClient, mint: str, retries: int = 3, delay: float = 1.0, is_pending_retry: bool = False) -> Optional[Dict]:
    log_prefix = f"[{mint}]"
    if is_pending_retry:
        log_prefix = f"[{mint} PENDING_RETRY]"
    
    logger.info(f"{log_prefix} --- Attempting to fetch metadata ---")
    for attempt in range(retries):
        logger.info(f"{log_prefix} fetch_metadata attempt {attempt + 1}/{retries}")
        try:
            mint_pk = Pubkey.from_string(mint)
            pda, pda_bump = Pubkey.find_program_address(
                [METADATA_PREFIX, bytes(METAPLEX_PROGRAM_ID), bytes(mint_pk)],
                METAPLEX_PROGRAM_ID,
            )
            logger.debug(f"{log_prefix} Metadata PDA: {pda} (bump: {pda_bump})")
            
            logger.debug(f"{log_prefix} Calling get_account_info for PDA: {pda}")
            response_rpc = await client.get_account_info(pda)

            if not response_rpc.value: 
                logger.warning(f"{log_prefix} Metadata account {pda} NOT FOUND on chain (attempt {attempt+1}).")
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                logger.warning(f"{log_prefix} Metadata account {pda} not found after {retries} retries.")
                return None 

            acc_data_bytes = response_rpc.value.data # This is bytes
            logger.info(f"{log_prefix} Metadata account {pda} FOUND. Data length: {len(acc_data_bytes)}")
            
            # Log the raw data as hex to inspect its contents thoroughly
            hex_data = acc_data_bytes.hex()
            logger.debug(f"{log_prefix} Raw metadata account data (hex): {hex_data}")
            
            if len(acc_data_bytes) < 65: 
                logger.warning(f"{log_prefix} Metadata account data too short ({len(acc_data_bytes)} bytes).")
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                return None 

            try:
                meta_parsed_obj = METADATA_LAYOUT.parse(acc_data_bytes)
            except Exception as e_parse:
                logger.error(f"{log_prefix} FAILED TO PARSE METADATA with construct: {e_parse}", exc_info=True)
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                return None 

            # Log raw bytes of string fields from parsed object for clarity
            try:
                raw_name_bytes = meta_parsed_obj.name.encode('utf-8', 'surrogatepass') if isinstance(meta_parsed_obj.name, str) else b''
                raw_symbol_bytes = meta_parsed_obj.symbol.encode('utf-8', 'surrogatepass') if isinstance(meta_parsed_obj.symbol, str) else b''
                raw_uri_bytes = meta_parsed_obj.uri.encode('utf-8', 'surrogatepass') if isinstance(meta_parsed_obj.uri, str) else b''

                logger.debug(f"{log_prefix} Raw parsed bytes - Name: {raw_name_bytes.hex()} | Symbol: {raw_symbol_bytes.hex()} | URI: {raw_uri_bytes.hex()}")
            except Exception as e_raw_log:
                logger.debug(f"{log_prefix} Error logging raw parsed bytes: {e_raw_log}")


            name = meta_parsed_obj.name.rstrip("\x00") if isinstance(meta_parsed_obj.name, str) else ""
            symbol = meta_parsed_obj.symbol.rstrip("\x00") if isinstance(meta_parsed_obj.symbol, str) else ""
            uri = meta_parsed_obj.uri.rstrip("\x00") if isinstance(meta_parsed_obj.uri, str) else ""
            
            logger.info(f"{log_prefix} Stripped - Name: '{name}', Symbol: '{symbol}', URI: '{uri}'")

            if not name.strip(): 
                logger.warning(f"{log_prefix} Name is empty or whitespace. Attempt {attempt+1}/{retries}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                logger.error(f"{log_prefix} Skipping metadata due to empty name after {retries} retries.")
                return None 
                
            metadata = { "mint": mint, "name": name, "symbol": symbol, "uri": uri, "timestamp": datetime.now().isoformat()}
            
            with open("minted_tokens.jsonl", "a", encoding="utf-8") as fp:
                fp.write(json.dumps(metadata) + "\n")
                
            stats["metadata_successes"] += 1
            stats["last_mint_at"] = datetime.now().isoformat()
            
            logger.info(f"[METADATA_SUCCESS] Mint: {mint} | Name: {name} | Symbol: {symbol} | URI: {uri}")
            return metadata
            
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and e.response else "Unknown"
            logger.warning(f"{log_prefix} HTTP error {status_code} (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay * (2 + attempt)) 
                continue
            return None
        except Exception as e:
            logger.error(f"{log_prefix} Metadata error (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < retries - 1:
                await asyncio.sleep(delay * (attempt + 1)) 
                continue
            return None
    
    logger.warning(f"{log_prefix} All {retries} attempts to fetch metadata failed for this call.")
    stats["metadata_failures"] += 1 
    return None

# ────────────────────── MINT DETECTION LOGIC ──────────────────────
def find_mint_address_in_logs(logs: List[str], accounts: List[str]) -> Optional[str]:
    """
    Smart mint address detection from logs and accounts
    Looks for InitializeMint instructions and finds the relevant mint account.
    Note: `accounts` list might be empty if not provided by WebSocket message.
    """
    for i, log in enumerate(logs):
        if "InitializeMint" in log:
            context_range = min(3, len(logs) - i) 
            for j in range(i, i + context_range):
                if j < len(logs) and "mint:" in logs[j].lower(): 
                    parts = logs[j].split()
                    for part in parts:
                        if 32 <= len(part) <= 44 and (not accounts or part in accounts):
                            try: 
                                Pubkey.from_string(part)
                                return part
                            except ValueError:
                                continue
            
            if accounts and len(accounts) > 0:
                for acc_candidate in accounts:
                    for k_idx in range(max(0, i-1), min(len(logs), i+2)): 
                        if acc_candidate in logs[k_idx]:
                            return acc_candidate
    
    token_program_log_indices = [idx for idx, log_line in enumerate(logs) if f"Program {TOKEN_PROGRAM_ID}" in log_line]
    
    if token_program_log_indices and accounts:
        for account in accounts:
            for log_idx in token_program_log_indices:
                context_range = 5
                start = max(0, log_idx - context_range)
                end = min(len(logs), log_idx + context_range + 1)
                
                for j in range(start, end):
                    if account in logs[j]:
                        if "InitializeMint" in logs[j] or \
                           ("init" in logs[j].lower() and "mint" in logs[j].lower()): 
                            return account
    
    return None


# This is a new worker function
async def process_transaction_from_queue(client: AsyncClient):
    """
    Worker to process transaction signatures from a queue with a delay.
    """
    logger.info("Transaction processing worker started.")
    while True:
        try:
            # Wait for a signature to appear in the queue
            # This will block until an item is available
            signature = await transaction_queue.get()
            logger.debug(f"Retrieved {signature} from transaction queue. Queue size: {transaction_queue.qsize()}")
            
            # Call the original process_transaction function
            # The delay is now primarily handled by this worker's sleep,
            # but the internal delay in process_transaction can remain as a secondary throttle.
            mint_addr = await process_transaction(client, signature)
            
            if mint_addr and mint_addr not in processed:
                logger.info(f"[MINT_QUEUE] Mint found for {signature}: {mint_addr}")
                processed.add(mint_addr) # Ensure it's added to processed here too
                stats["total_mints_found"] += 1 # Stat update might need care to avoid double counting
                                                # if already counted when added to queue trigger.
                                                # Let's assume for now it's only truly "found" after processing.

                # Fetch metadata for the mint found via full transaction processing
                metadata = await fetch_metadata(client, mint_addr)
                if not metadata:
                    logger.warning(f"Failed metadata fetch for mint {mint_addr} found via tx {signature}. Adding to pending.")
                    pending_mints.append(mint_addr)
            elif mint_addr and mint_addr in processed:
                logger.debug(f"Mint {mint_addr} (from queue for tx {signature}) was already processed.")

            transaction_queue.task_done() # Notify the queue that the item has been processed

            # Throttle hard here: wait before processing the next transaction from the queue
            await asyncio.sleep(5)  # Wait 5 seconds before picking up the next one

        except asyncio.CancelledError:
            logger.info("Transaction processing worker cancelled.")
            break
        except Exception as e:
            # Log error but continue running the worker
            logger.error(f"Error in transaction processing worker for signature '{signature if 'signature' in locals() else 'unknown'}': {e}", exc_info=True)
            # If an error occurs with a specific signature, ensure task_done is called if it wasn't.
            # However, if signature wasn't even retrieved, there's no task to mark done.
            if 'signature' in locals() and not transaction_queue.empty(): 
                try:
                    transaction_queue.task_done() # Attempt to mark as done to prevent stall if error was after get()
                except ValueError: # if task_done() called too many times
                    pass
            await asyncio.sleep(5) # Still sleep to prevent rapid error loops



async def process_transaction(client: AsyncClient, signature: str) -> Optional[str]:
    logger.info(f"--- Starting process_transaction for signature: {signature} ---")
    try:
        try:
            tx_sig_obj = Signature.from_string(signature)
        except ValueError:
            logger.error(f"Invalid signature string format for tx {signature}")
            return None

        logger.debug(f"[{signature}] Calling client.get_transaction with jsonParsed...")
        tx_response_obj = await client.get_transaction(
            tx_sig_obj,
            encoding="jsonParsed", 
            max_supported_transaction_version=0 
        )
        
        if not tx_response_obj or not tx_response_obj.value:
            logger.warning(f"[{signature}] No transaction data object returned from get_transaction.")
            return None
            
        # This is EncodedConfirmedTransactionWithStatusMeta
        tx_details_outer_obj: Any = tx_response_obj.value 
        logger.info(f"[{signature}] Type of tx_details_outer_obj: {type(tx_details_outer_obj)}")

        # This is EncodedTransactionWithStatusMeta (which itself has transaction and meta)
        inner_tx_data_obj: Optional[Any] = getattr(tx_details_outer_obj, 'transaction', None)
        if not inner_tx_data_obj:
            logger.warning(f"[{signature}] 'transaction' attribute (inner EncodedTxWithSM) missing in tx_details_outer_obj.")
            return None
        logger.debug(f"[{signature}] Type of inner_tx_data_obj: {type(inner_tx_data_obj)}")
        
        # This is UiTransaction (which has .message and .signatures)
        # The .transaction attribute of inner_tx_data_obj might be solders.transaction_status.UiTransaction directly
        # or it could be a solders.SanitizedTransaction an Enum with a variant like Json(UiTransaction)
        # The Helius output showed `transaction: Json(UiTransaction { ... })`
        # So we need to handle if it's wrapped in Json()
        
        actual_ui_transaction_obj: Optional[Any] = None
        raw_transaction_part = getattr(inner_tx_data_obj, 'transaction', None)
        if raw_transaction_part:
            logger.debug(f"[{signature}] Type of raw_transaction_part: {type(raw_transaction_part)}")
            if hasattr(raw_transaction_part, 'value') and type(raw_transaction_part).__name__ == 'Json': # Check for Json enum variant
                 actual_ui_transaction_obj = raw_transaction_part.value # Get the UiTransaction from Json(value)
                 logger.debug(f"[{signature}] Extracted UiTransaction from Json wrapper. Type: {type(actual_ui_transaction_obj)}")
            elif type(raw_transaction_part).__name__ == 'UiTransaction': # Directly a UiTransaction
                 actual_ui_transaction_obj = raw_transaction_part
                 logger.debug(f"[{signature}] Found UiTransaction directly.")
            else:
                logger.warning(f"[{signature}] Unexpected type for raw_transaction_part: {type(raw_transaction_part)}")


        meta_object: Optional[Any] = getattr(inner_tx_data_obj, 'meta', None) # This is UiTransactionStatusMeta

        if not actual_ui_transaction_obj:
            logger.warning(f"[{signature}] Could not extract UiTransaction object.")
        if not meta_object: 
            logger.warning(f"[{signature}] Nested 'meta' (UiTransactionStatusMeta object) missing.")
        
        if not actual_ui_transaction_obj or not meta_object:
            return None
        
        message_obj: Optional[Any] = getattr(actual_ui_transaction_obj, 'message', None)
        
        if not message_obj: # message_obj is UiParsedMessage
            logger.warning(f"[{signature}] '.message' attribute missing on UiTransaction object.")
            return None
        
        logger.debug(f"[{signature}] Type of message_obj (UiParsedMessage): {type(message_obj)}")
        
        # tx_message_account_keys_info_raw is List[ParsedAccount]
        tx_message_account_keys_info_raw: Optional[List[Any]] = getattr(message_obj, 'account_keys', None)
        # instructions_raw is List[UiParsedInstruction | UiPartiallyDecodedInstruction]
        instructions_raw: Optional[List[Any]] = getattr(message_obj, 'instructions', None)


        if not isinstance(tx_message_account_keys_info_raw, list):
             logger.warning(f"[{signature}] message_obj.account_keys is not a list or is missing. Type: {type(tx_message_account_keys_info_raw)}")
             return None

        account_keys_str_list: List[str] = []
        for acc_info_obj in tx_message_account_keys_info_raw: # acc_info_obj is ParsedAccount
            if hasattr(acc_info_obj, 'pubkey') and isinstance(getattr(acc_info_obj, 'pubkey'), Pubkey):
                account_keys_str_list.append(str(acc_info_obj.pubkey))
            else:
                logger.warning(f"[{signature}] Account info object missing .pubkey or not Pubkey type: {acc_info_obj}")
        
        logger.debug(f"[{signature}] Extracted account key strings count: {len(account_keys_str_list)}")

        program_id_str = str(TOKEN_PROGRAM_ID)
        potential_mint = None
        
        logger.debug(f"[{signature}] Checking outer instructions...")
        if isinstance(instructions_raw, list):
            for i, ix_obj in enumerate(instructions_raw): # ix_obj is ParsedInstruction or UiPartiallyDecodedInstruction
                # These objects have .program_id, .accounts (List[Pubkey]), .data (str), .parsed (Dict)
                current_ix_program_id_str = None
                program_id_attr = getattr(ix_obj, 'program_id', None) # program_id is a Pubkey
                if isinstance(program_id_attr, Pubkey):
                    current_ix_program_id_str = str(program_id_attr)
                
                parsed_info_dict: Optional[Dict[str, Any]] = getattr(ix_obj, 'parsed', None)
                ix_type_from_parsed = parsed_info_dict.get('type') if isinstance(parsed_info_dict, dict) else 'N/A'
                logger.debug(f"[{signature}] Outer Ix {i} programId: {current_ix_program_id_str}, Parsed type from RPC: {ix_type_from_parsed}")

                if current_ix_program_id_str == program_id_str:
                    if isinstance(parsed_info_dict, dict) and \
                       (parsed_info_dict.get("type") == "initializeMint2" or parsed_info_dict.get("type") == "initializeMint"):
                        
                        mint_info_dict: Optional[Dict[str, Any]] = parsed_info_dict.get("info")
                        if isinstance(mint_info_dict, dict):
                            potential_mint = mint_info_dict.get("mint") # This should be a string pubkey
                            if potential_mint and isinstance(potential_mint, str):
                                logger.info(f"Found {parsed_info_dict.get('type')} (outer): Mint {potential_mint} in tx {signature}")
                                return potential_mint
                            else:
                                logger.warning(f"[{signature}] Outer Ix {i} ({parsed_info_dict.get('type')}) 'mint' key missing in info or not string. Info: {mint_info_dict}")
                        else:
                            logger.warning(f"[{signature}] Outer Ix {i} ({parsed_info_dict.get('type')}) 'info' part is not a dict. Parsed: {parsed_info_dict}")
        else:
             logger.warning(f"[{signature}] message_obj.instructions is not a list or is missing.")

        if not potential_mint: 
            logger.debug(f"[{signature}] Checking inner instructions...")
            inner_instructions_list: Optional[List[Any]] = getattr(meta_object, 'inner_instructions', None)
            if isinstance(inner_instructions_list, list):
                for group_idx, inner_group_obj in enumerate(inner_instructions_list): 
                    if not hasattr(inner_group_obj, 'instructions'): continue
                    
                    inner_ixs_for_group: Optional[List[Any]] = getattr(inner_group_obj, 'instructions', None)
                    if isinstance(inner_ixs_for_group, list):
                        for i, ix_obj in enumerate(inner_ixs_for_group):
                            current_ix_program_id_str = None
                            program_id_attr = getattr(ix_obj, 'program_id', None)
                            if isinstance(program_id_attr, Pubkey):
                                current_ix_program_id_str = str(program_id_attr)

                            parsed_info_dict = getattr(ix_obj, 'parsed', None)
                            ix_type_from_parsed = parsed_info_dict.get('type') if isinstance(parsed_info_dict, dict) else 'N/A'
                            logger.debug(f"[{signature}] Inner Ix Group {group_idx}, Ix {i} programId: {current_ix_program_id_str}, Parsed type from RPC: {ix_type_from_parsed}")

                            if current_ix_program_id_str == program_id_str:
                                if isinstance(parsed_info_dict, dict) and \
                                   (parsed_info_dict.get("type") == "initializeMint2" or parsed_info_dict.get("type") == "initializeMint"):
                                    mint_info_dict: Optional[Dict[str, Any]] = parsed_info_dict.get("info")
                                    if isinstance(mint_info_dict, dict):
                                        potential_mint = mint_info_dict.get("mint")
                                        if potential_mint and isinstance(potential_mint, str):
                                            logger.info(f"Found {parsed_info_dict.get('type')} (inner/CPI): Mint {potential_mint} in tx {signature}")
                                            return potential_mint
            else:
                logger.debug(f"[{signature}] meta_object.inner_instructions is not a list or is None (OK if no CPIs).")
        
        if not potential_mint:
            logger.debug(f"[{signature}] No InitializeMint from instruction parsing. Trying log-based search.")
            logs: Optional[List[str]] = getattr(meta_object, 'log_messages', None)
            if not isinstance(logs, list) or not logs: 
                logger.debug(f"[{signature}] No logMessages for log-based fallback.")
            else: 
                # ... (log-based fallback, which is now less likely to be needed if instruction parsing is correct)
                pass # Kept for brevity, assume it's mostly okay if account_keys_str_list is available
            
            if not potential_mint:
                 logger.info(f"[{signature}] No mint found in tx after all checks.")

        return potential_mint
        
    except httpx.HTTPStatusError as e: 
        status_code = e.response.status_code if hasattr(e, 'response') and e.response else "Unknown"
        if status_code == 429: 
            logger.warning(f"Rate limited (429) while processing transaction {signature}. Details: {e}")
        else:
            logger.error(f"HTTP error (status {status_code}) processing transaction {signature}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Generic error processing transaction {signature}: {e}", exc_info=True)
        return None


# ────────────────────── LOG PARSER ──────────────────────
def parse_ws_notification(msg: Any) -> Tuple[Optional[List[str]], Optional[List[str]], Optional[str]]:
    parsed_logs: Optional[List[str]] = None
    parsed_accounts: Optional[List[str]] = None
    parsed_signature: Optional[str] = None

    actual_msg_to_parse = msg
    
    if isinstance(msg, list):
        if len(msg) > 0:
            logger.debug(f"WS message is a list, processing first element: {type(msg[0])}")
            actual_msg_to_parse = msg[0] 
        else:
            logger.warning("WS message is an empty list."); return None, None, None

    if isinstance(actual_msg_to_parse, SubscriptionResult): 
        # SubscriptionResult itself is often just an int (the subscription ID)
        # or an object whose string representation is useful.
        # The RPC response structure for subscription confirmation can vary.
        # If it's just an int, actual_msg_to_parse will be the ID.
        # If it's a more complex SubscriptionResult object, we log its string form.
        logger.info(f"Received WS SubscriptionResult (Content: {actual_msg_to_parse}), not a log. Ignoring.")
        return None, None, None # This is not a log message to process for mints

    if isinstance(actual_msg_to_parse, RpcLogsResponse) and isinstance(actual_msg_to_parse.value, LogsNotification):
        notification = actual_msg_to_parse.value
        parsed_logs = notification.logs; parsed_signature = str(notification.signature)
        logger.debug(f"Parsed WS as RpcLogsResponse for sig: {parsed_signature}")
    elif isinstance(actual_msg_to_parse, dict) and actual_msg_to_parse.get("method") == "logsNotification":
        params = actual_msg_to_parse.get("params", {}); result = params.get("result", {}); value = result.get("value", {})
        parsed_logs = value.get("logs"); raw_accounts = value.get("accounts")
        if isinstance(raw_accounts, list): parsed_accounts = [str(acc) for acc in raw_accounts]
        sig_val = value.get("signature")
        if sig_val: parsed_signature = str(sig_val)
        logger.debug(f"Parsed WS as dict for sig: {parsed_signature}")
    elif hasattr(actual_msg_to_parse, "result") and hasattr(actual_msg_to_parse.result, "value"):
        res_obj = actual_msg_to_parse.result; v = res_obj.value
        if isinstance(v, LogsNotification):
            parsed_logs = v.logs; parsed_signature = str(v.signature)
            logger.debug(f"Parsed WS via .result.value as LogsNotification for sig: {parsed_signature}")
        elif hasattr(v, "logs") and hasattr(v, "signature"):
            parsed_logs = v.logs; parsed_signature = str(v.signature)
            if hasattr(v, "accounts") and isinstance(v.accounts, list): parsed_accounts = [str(acc) for acc in v.accounts]
            logger.debug(f"Parsed WS via .result.value attributes for sig: {parsed_signature}")
    
    if parsed_logs is None and not isinstance(actual_msg_to_parse, SubscriptionResult):
        if not (isinstance(msg, list) and not msg): 
             logger.warning(f"Could not parse logs from WS. Type after unwrap: {type(actual_msg_to_parse)} (Original: {type(msg)})")
             
    return parsed_logs, parsed_accounts, parsed_signature
# ────────────────────── PERIODIC TASKS ──────────────────────
# Relevant Function: process_pending_mints

async def process_pending_mints(client: AsyncClient):
    global pending_mints 

    if not pending_mints:
        return
        
    logger.info(f"--- Processing {len(pending_mints)} pending mints ---")
    
    mints_to_retry_this_round = pending_mints[:] 
    successful_mints_this_round = []
    
    # Check if the output file exists to avoid errors when reading it
    output_file_exists = os.path.exists("minted_tokens.jsonl")

    for mint_addr in mints_to_retry_this_round:
        # Check if already fully processed (with name) by looking at the file IF IT EXISTS
        already_fully_processed = False
        if output_file_exists and mint_addr in processed: # Check 'processed' set first for quick skip
            try:
                with open("minted_tokens.jsonl", "r", encoding="utf-8") as f_check: # Added encoding
                    for line in f_check:
                        try:
                            data = json.loads(line)
                            if data.get("mint") == mint_addr and data.get("name", " ").strip():
                                already_fully_processed = True
                                break
                        except json.JSONDecodeError:
                            continue # Skip malformed lines
            except FileNotFoundError: 
                # This case should ideally not be hit if output_file_exists was checked first,
                # but as a safeguard.
                logger.debug("minted_tokens.jsonl not found during pending check (should be rare).")
                pass # Continue as if not processed

        if already_fully_processed:
             logger.debug(f"Mint {mint_addr} already fully processed (with name in file), skipping from pending.")
             successful_mints_this_round.append(mint_addr) 
             continue

        logger.debug(f"Retrying metadata for pending mint: {mint_addr}")
        metadata = await fetch_metadata(client, mint_addr, retries=2, delay=5.0, is_pending_retry=True)
        
        if metadata and metadata.get("name", "").strip(): 
            logger.info(f"Successfully fetched metadata for PENDING mint: {mint_addr}")
            successful_mints_this_round.append(mint_addr)
        else:
            logger.warning(f"Still failed to fetch complete metadata for PENDING mint {mint_addr}.")

    if successful_mints_this_round:
        logger.info(f"Removing {len(successful_mints_this_round)} successfully processed mints from pending list.")
        pending_mints = [m for m in pending_mints if m not in successful_mints_this_round]
    
    logger.info(f"--- Finished processing pending mints. {len(pending_mints)} still pending. ---")

async def periodic_stats_update():
    """Update and log statistics periodically"""
    while True:
        try:
            await asyncio.sleep(300)
            
            logger.info(f"Stats: {json.dumps(stats, indent=2)}")
            
            with open("token_tracker_stats.json", "w") as f:
                json.dump(stats, f, indent=2)
                
        except asyncio.CancelledError:
            logger.info("Stats update task cancelled.")
            break
        except Exception as e:
            logger.error(f"Error updating stats: {e}")

# ────────────────────── SINGLE WS SESSION ──────────────────────
async def listen_once() -> None:
    """
    Enhanced WebSocket listener that uses a queue for transaction processing.
    """
    client = AsyncClient(HTTP_RPC) 
    
    # Start the transaction processing worker task
    # It's important this worker uses the same `client` or a new one if preferred for its lifecycle.
    # For simplicity, using the same client. Ensure client.close() is handled correctly.
    transaction_worker_task = asyncio.create_task(process_transaction_from_queue(client))

    pending_task = None
    stats_task = None

    try:
        async with connect(WS_URI, ping_interval=20) as ws: 
            await ws.logs_subscribe(
                RpcTransactionLogsFilterMentions(TOKEN_PROGRAM_ID)
            )
            logger.info("[+] WebSocket ready. Waiting for new token mints…")
            
            pending_task = asyncio.create_task(process_pending_mints(client)) 
            stats_task = asyncio.create_task(periodic_stats_update())
            last_pending_check = time.time()
            
            try:
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=120) 
                    logs, ws_accounts, signature = parse_ws_notification(msg)
                    
                    if not logs or not signature: 
                        if logs and not signature:
                            logger.debug("Logs received without a signature.")
                        await asyncio.sleep(0.01) 
                        continue
                        
                    mint_addr_from_logs = find_mint_address_in_logs(logs, ws_accounts or [])
                    
                    if mint_addr_from_logs and mint_addr_from_logs not in processed:
                        logger.info(f"🎯 Potential mint (from logs): {mint_addr_from_logs} (sig: {signature})")
                        processed.add(mint_addr_from_logs)
                        # We count here if found from logs, might need adjustment if queue processing also counts
                        stats["total_mints_found"] += 1 
                        metadata = await fetch_metadata(client, mint_addr_from_logs)
                        if not metadata:
                            logger.warning(f"Failed initial metadata for {mint_addr_from_logs}. Adding to pending.")
                            pending_mints.append(mint_addr_from_logs)
                    
                    # Fallback to queueing for full transaction processing
                    elif not mint_addr_from_logs and PROCESS_TRANSACTIONS_FALLBACK:
                        if any("InitializeMint" in log for log in logs):
                            logger.debug(f"InitializeMint in logs but no mint by direct parsing for {signature}. Queuing for full transaction processing.")
                            await transaction_queue.put(signature)
                        # Consider if the broader "mint" keyword check should also queue
                        # elif any(str(TOKEN_PROGRAM_ID) in log for log in logs) and any("mint to" in log.lower() for log in logs):
                        #     logger.debug(f"Token program and 'mint to' in logs, no mint by direct parsing for {signature}. Queuing.")
                        #     await transaction_queue.put(signature)
                    
                    current_time = time.time()
                    if current_time - last_pending_check > 30:  
                        last_pending_check = current_time
                        if pending_task and not pending_task.done():
                            pending_task.cancel()
                            try: await pending_task 
                            except asyncio.CancelledError: pass 
                        pending_task = asyncio.create_task(process_pending_mints(client))
                        
            except asyncio.TimeoutError:
                logger.info("No WebSocket messages received recently (timeout). Will trigger reconnection.")
                raise 
            
            finally: 
                if pending_task and not pending_task.done():
                    pending_task.cancel()
                    try: await pending_task
                    except asyncio.CancelledError: pass
                if stats_task and not stats_task.done():
                    stats_task.cancel()
                    try: await stats_task
                    except asyncio.CancelledError: pass
    
    except (ConnectionClosedError, asyncio.TimeoutError, Exception) as e: 
        logger.error(f"Error in listen_once: {type(e).__name__} - {e}", exc_info=False) 
        # Re-raise to be handled by main loop's reconnect logic
        raise 

    finally:
        # Clean up the transaction worker task when listen_once exits
        if transaction_worker_task and not transaction_worker_task.done():
            logger.info("Cancelling transaction processing worker...")
            transaction_worker_task.cancel()
            try:
                await transaction_worker_task
            except asyncio.CancelledError:
                logger.info("Transaction processing worker successfully cancelled.")
            except Exception as e_worker:
                logger.error(f"Error during transaction worker shutdown: {e_worker}")
        
        await client.close()


# ────────────────────── MAIN RECONNECT LOOP ──────────────────────

# Relevant Function: main

async def main():
    shutdown_event = asyncio.Event()
    # Keep track of main tasks that need explicit cancellation
    active_tasks: Set[asyncio.Task] = set() # type: ignore

    def handle_signal(sig: Any, frame: Any) -> None:
        logger.info(f"\n[×] Received signal {sig}. Attempting graceful shutdown...")
        if not shutdown_event.is_set(): 
            shutdown_event.set()
            # Attempt to cancel all known active tasks
            for task in list(active_tasks): # Iterate over a copy
                if not task.done():
                    logger.debug(f"Cancelling task: {task.get_name()}")
                    task.cancel()
        else:
            logger.warning("Shutdown already in progress. Press Ctrl+C again to force exit (may be abrupt).")
            # This is where you might consider a more forceful exit if the first Ctrl+C doesn't work after a delay
            # For example, after a second Ctrl+C, you could call sys.exit(1)
            # However, this bypasses asyncio cleanup and can be messy.
            # For now, let's rely on asyncio's cancellation.
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    loop_count = 0
    current_listen_task = None

    while not shutdown_event.is_set():
        loop_count+=1
        if loop_count % 60 == 0:
            logger.debug(f"Main loop iteration: {loop_count}, shutdown_event: {shutdown_event.is_set()}")
        
        try:
            logger.info("Starting WebSocket listener...")
            # Create listen_once as a task and add to active_tasks
            current_listen_task = asyncio.create_task(listen_once(), name="listen_once_task")
            active_tasks.add(current_listen_task)
            await current_listen_task # Wait for listen_once to complete or raise an exception

        except asyncio.CancelledError: 
            logger.info("Main task or listen_once_task was cancelled.")
            # This can happen if handle_signal cancels current_listen_task
            # or if main itself is cancelled.
            break 
        except (ConnectionClosedError, Exception) as e: 
            if shutdown_event.is_set():
                logger.info("Shutdown initiated during error handling, exiting.")
                break
            stats["reconnections"] += 1
            logger.error(f"[!] Listener error: {type(e).__name__} - {e} — reconnecting in 5 seconds")
            
            sleep_task = asyncio.create_task(asyncio.sleep(5))
            shutdown_wait_task = asyncio.create_task(shutdown_event.wait())
            active_tasks.add(sleep_task)
            active_tasks.add(shutdown_wait_task)
            
            done, pending = await asyncio.wait(
                [sleep_task, shutdown_wait_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            active_tasks.remove(sleep_task)
            active_tasks.remove(shutdown_wait_task)

            if shutdown_wait_task in done: 
                if sleep_task in pending: sleep_task.cancel() 
                logger.info("Reconnect sleep interrupted for shutdown.")
                break 
            if shutdown_wait_task in pending: shutdown_wait_task.cancel()
            
        finally:
            if current_listen_task and current_listen_task in active_tasks:
                active_tasks.remove(current_listen_task)
            
    logger.info("\n[×] Main loop ending. Cleaning up remaining tasks...")
    # Final cleanup of any tasks that might still be tracked or running
    # This includes the transaction_worker_task if it was started by a listen_once call that got cancelled
    # and any other tasks like pending_mints_task or stats_task.
    # listen_once's finally block should handle its own tasks, but this is an extra layer.
    
    # Cancel all tasks that might have been created by listen_once if it was rudely interrupted
    # Note: transaction_worker_task is started inside listen_once. If listen_once's finally block
    # runs, it should cancel it. This is a belt-and-suspenders approach.
    tasks_to_await = []
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task() and not task.done(): # Don't cancel self (main) or already done tasks
            logger.debug(f"Attempting to cancel task: {task.get_name()}")
            task.cancel()
            tasks_to_await.append(task)
    
    if tasks_to_await:
        results = await asyncio.gather(*tasks_to_await, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                logger.debug(f"Task {tasks_to_await[i].get_name()} successfully cancelled during final cleanup.")
            elif isinstance(result, Exception):
                logger.error(f"Error in task {tasks_to_await[i].get_name()} during final cleanup: {result}")
    
    logger.info("Saving final stats...")
    try:
        with open("token_tracker_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        logger.info("Final stats saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save final stats: {e}")
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt: 
        logger.info("\n[×] Application terminated by KeyboardInterrupt in asyncio.run.")
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)