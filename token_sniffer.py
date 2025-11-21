#!/usr/bin/env python
# Enhanced SPL token tracker with robust mint detection
# Captures both direct and CPI-initiated mints
# Includes smart account indexing and connection recovery
# Saves data to minted_tokens.jsonl
import os
from dotenv import load_dotenv
import asyncio
import json
import logging
import signal
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from solders.transaction_status import UiTransactionEncoding
from solders.pubkey import Pubkey
from solders.signature import Signature
import httpx
from solders.rpc.config import RpcTransactionLogsFilterMentions, RpcTransactionLogsFilter
from solders.rpc.responses import RpcLogsResponse, LogsNotification, SubscriptionResult, RPCError, LogsNotificationResult # Corrected to LogsNotificationResult
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.rpc.websocket_api import SolanaWsClientProtocol, connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK # type: ignore
from construct import Int8ul, Bytes, Struct, CString # type: ignore

load_dotenv()

PROCESS_TRANSACTIONS_FALLBACK = True
transaction_queue: asyncio.Queue[str] = asyncio.Queue()

# Near the top of your script, after imports
logging.basicConfig(
    level=logging.INFO, # Keep your general level at INFO or DEBUG
    format="%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(lineno)d - %(message)s", # Added name, module, lineno
    handlers=[
        logging.FileHandler("token_tracker.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TokenTracker") # Your main logger

# Specifically enable DEBUG logging for the websockets library
# This will show you the raw frames being sent and received
ws_logger = logging.getLogger('websockets')
ws_logger.setLevel(logging.DEBUG)
ws_logger.addHandler(logging.StreamHandler()) # Ensure it prints to console

TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN_2022_PROGRAM_ID = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
METAPLEX_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
METADATA_PREFIX = b"metadata"

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not HELIUS_API_KEY:
    logger.error("HELIUS_API_KEY not found. Please set it in .env or environment variables.")
    logger.warning("Proceeding with public RPC endpoints. This is not recommended for production.")
    WS_URI = "wss://api.mainnet-beta.solana.com/"
    HTTP_RPC = "https://api.mainnet-beta.solana.com"
else:
    logger.info("Using Helius RPC endpoints.")
    WS_URI = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    HTTP_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

processed: Set[str] = set()
try:
    with open("minted_tokens.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line)
                if mint_val := data.get("mint"):
                    processed.add(mint_val)
            except json.JSONDecodeError:
                logger.warning(f"Skipping malformed line in minted_tokens.jsonl: {line.strip()}")
                continue
    logger.info(f"Loaded {len(processed)} previously processed mints")
except FileNotFoundError:
    logger.info("No previous mint file found, starting fresh")

stats = {
    "started_at": datetime.now().isoformat(), "total_mints_found": 0,
    "metadata_successes": 0, "metadata_failures": 0, "reconnections": 0,
    "last_mint_at": None, "ws_subscriptions_active": 0
}
METADATA_LAYOUT = Struct(
    "key" / Int8ul, "update_authority" / Bytes(32), "mint" / Bytes(32),
    "name" / CString("utf8"), "symbol" / CString("utf8"), "uri" / CString("utf8"),
)
pending_mints: List[str] = []

async def fetch_metadata(client: AsyncClient, mint: str, retries: int = 3, delay: float = 1.0, is_pending_retry: bool = False) -> Optional[Dict[str, Any]]:
    """Fetches and decodes Metaplex token metadata for a given mint address."""
    log_prefix = f"[{mint}]"
    if is_pending_retry: log_prefix = f"[{mint} PENDING_RETRY]"
    logger.info(f"{log_prefix} --- Attempting to fetch metadata ---")
    for attempt in range(retries):
        logger.info(f"{log_prefix} fetch_metadata attempt {attempt + 1}/{retries}")
        try:
            mint_pk = Pubkey.from_string(mint)
            pda, _ = Pubkey.find_program_address(
                [METADATA_PREFIX, bytes(METAPLEX_PROGRAM_ID), bytes(mint_pk)], METAPLEX_PROGRAM_ID)
            logger.debug(f"{log_prefix} Metadata PDA: {pda}")
            response_rpc = await client.get_account_info(pda, commitment=Confirmed)
            if not response_rpc.value:
                logger.warning(f"{log_prefix} Metadata account {pda} NOT FOUND (attempt {attempt+1}).")
                if attempt < retries - 1: await asyncio.sleep(delay * (attempt+1)); continue
                logger.warning(f"{log_prefix} Metadata account {pda} not found after {retries} retries."); return None
            acc_data_bytes = response_rpc.value.data
            logger.info(f"{log_prefix} Metadata account {pda} FOUND. Data length: {len(acc_data_bytes)}")
            logger.debug(f"{log_prefix} Raw metadata (hex): {acc_data_bytes.hex()[:128]}...")
            if len(acc_data_bytes) < 65: # Minimum size check
                logger.warning(f"{log_prefix} Metadata data too short ({len(acc_data_bytes)} bytes).")
                if attempt < retries - 1: await asyncio.sleep(delay * (attempt+1)); continue
                return None
            try: meta_parsed_obj = METADATA_LAYOUT.parse(acc_data_bytes)
            except Exception as e_parse:
                logger.error(f"{log_prefix} FAILED TO PARSE METADATA: {e_parse}", exc_info=True)
                if attempt < retries - 1: await asyncio.sleep(delay * (attempt+1)); continue
                return None
            name = meta_parsed_obj.name.rstrip("\x00") if isinstance(meta_parsed_obj.name, str) else ""
            symbol = meta_parsed_obj.symbol.rstrip("\x00") if isinstance(meta_parsed_obj.symbol, str) else ""
            uri = meta_parsed_obj.uri.rstrip("\x00") if isinstance(meta_parsed_obj.uri, str) else ""
            logger.info(f"{log_prefix} Stripped - Name: '{name}', Symbol: '{symbol}', URI: '{uri}'")
            if not name.strip(): # Crucial: Ensure name is not just whitespace
                logger.warning(f"{log_prefix} Name empty. Attempt {attempt+1}/{retries}")
                if attempt < retries - 1: await asyncio.sleep(delay * (attempt+1)); continue
                logger.error(f"{log_prefix} Skipping due to empty name after {retries} retries."); return None
            metadata_dict = {"mint": mint, "name": name, "symbol": symbol, "uri": uri, "timestamp": datetime.now().isoformat()}
            with open("minted_tokens.jsonl", "a", encoding="utf-8") as fp: fp.write(json.dumps(metadata_dict) + "\n")
            stats["metadata_successes"] += 1; stats["last_mint_at"] = datetime.now().isoformat()
            logger.info(f"METADATA_SUCCESS: Mint: {mint} | Name: {name} | Symbol: {symbol} | URI: {uri}")
            return metadata_dict
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and e.response else "Unknown"
            logger.warning(f"{log_prefix} HTTP error {status_code} (attempt {attempt+1}): {e}")
            if attempt < retries - 1: await asyncio.sleep(delay * (2+attempt)); continue
            return None
        except Exception as e:
            logger.error(f"{log_prefix} Metadata error (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < retries - 1: await asyncio.sleep(delay * (attempt+1)); continue
            return None
    logger.warning(f"{log_prefix} All {retries} attempts for metadata failed."); stats["metadata_failures"] += 1; return None

def find_mint_address_in_logs(logs: List[str], signature: str) -> Optional[str]:
    """
    Heuristic to find a mint address directly from log messages.
    Prefers "Program log: Initialize mint <MINT_ADDRESS>" style from Token-2022.
    Marks "Instruction: InitializeMint" for full parsing.
    """
    for log_entry in logs:
        if "Program log: Initialize mint " in log_entry: # Token-2022 specific log
            try:
                mint_candidate = log_entry.split("Program log: Initialize mint ")[1].split(" ")[0]
                Pubkey.from_string(mint_candidate) # Validate
                logger.debug(f"[{signature}] Found mint '{mint_candidate}' from Token-2022 log.")
                return mint_candidate
            except (IndexError, ValueError): 
                logger.debug(f"[{signature}] Could not parse mint from Token-2022 style log: {log_entry}")
                continue
        elif "Instruction: InitializeMint" in log_entry: # Generic SPL Token program log
            logger.debug(f"[{signature}] Saw 'Instruction: InitializeMint'. Indicating full tx parse is needed.")
            return "TBD_BY_FULL_PARSE" # Signal that full transaction parsing is required
    return None

async def process_transaction_from_queue(client: AsyncClient):
    """Worker to process transaction signatures from a queue to find mints via full transaction details."""
    logger.info("Transaction processing worker started.")
    while True:
        try:
            signature = await transaction_queue.get()
            logger.debug(f"Retrieved {signature} from tx_queue. Size: {transaction_queue.qsize()}")
            mint_addr = await process_transaction(client, signature)
            if mint_addr and mint_addr not in processed:
                logger.info(f"[MINT_QUEUE_SUCCESS] Mint from full parse for tx {signature}: {mint_addr}")
                processed.add(mint_addr) 
                # Defer counting total_mints_found to after metadata attempt in handle_log_notification or here
                metadata = await fetch_metadata(client, mint_addr)
                if metadata: # Successfully got metadata this time
                     if mint_addr not in stats.get("processed_with_metadata", set()): # Avoid double counting
                        stats["total_mints_found"] += 1
                        stats.setdefault("processed_with_metadata", set()).add(mint_addr)
                elif mint_addr not in pending_mints: # If metadata failed, add to pending if not already there
                    logger.warning(f"Failed metadata fetch for mint {mint_addr} (tx {signature}). Adding to pending.")
                    pending_mints.append(mint_addr)
            elif mint_addr and mint_addr in processed: 
                logger.debug(f"Mint {mint_addr} (tx {signature}) already processed.")
            elif not mint_addr: 
                logger.info(f"[MINT_QUEUE_FAIL] No mint from full parse for tx {signature}.")
            transaction_queue.task_done()
            await asyncio.sleep(2) # Throttle HTTP requests
        except asyncio.CancelledError: 
            logger.info("Transaction processing worker cancelled."); break
        except Exception as e:
            sig_ref = signature if 'signature' in locals() else 'unknown_sig_in_worker_error'
            logger.error(f"Error in tx processing worker for sig '{sig_ref}': {e}", exc_info=True)
            if 'signature' in locals() and not transaction_queue.empty(): # Check if signature was popped
                try: transaction_queue.task_done() # Try to mark as done to prevent stall
                except ValueError: pass # If already marked done by another path or too many calls
            await asyncio.sleep(5) # Wait before next attempt

async def process_transaction(client: AsyncClient, signature: str) -> Optional[str]:
    """Fetches and parses a full transaction to find InitializeMint instructions."""
    logger.info(f"--- Starting process_transaction for signature: {signature} ---")
    try:
        tx_sig_obj = Signature.from_string(signature)
        tx_response_obj = await client.get_transaction(
            tx_sig_obj, encoding=UiTransactionEncoding.JsonParsed,
            max_supported_transaction_version=0, commitment=Confirmed)
        if not tx_response_obj or not tx_response_obj.value:
            logger.warning(f"[{signature}] No transaction data from get_transaction."); return None
        
        inner_tx_data_obj = tx_response_obj.value.transaction # This is EncodedTransactionWithStatusMeta
        if not inner_tx_data_obj:
            logger.warning(f"[{signature}] 'transaction' attribute (EncodedTransactionWithStatusMeta) missing."); return None
        
        actual_ui_transaction_obj: Optional[Any] = None
        raw_transaction_part = inner_tx_data_obj.transaction # This is Union[UiTransaction, SanitizedTransaction]
        if raw_transaction_part:
            if hasattr(raw_transaction_part, 'value') and type(raw_transaction_part).__name__ == 'Json':
                 actual_ui_transaction_obj = raw_transaction_part.value # UiTransaction
            elif type(raw_transaction_part).__name__ == 'UiTransaction':
                 actual_ui_transaction_obj = raw_transaction_part
            else: 
                logger.warning(f"[{signature}] Unexpected type for raw_transaction_part: {type(raw_transaction_part)}"); return None
        else: 
            logger.warning(f"[{signature}] inner_tx_data_obj.transaction is None."); return None

        meta_object = inner_tx_data_obj.meta # UiTransactionStatusMeta
        if not actual_ui_transaction_obj or not meta_object:
            logger.warning(f"[{signature}] Missing UiTransaction or UiTransactionStatusMeta."); return None
        
        message_obj = getattr(actual_ui_transaction_obj, 'message', None) # UiMessage (can be UiParsedMessage)
        if not message_obj: 
            logger.warning(f"[{signature}] '.message' attribute missing on UiTransaction object."); return None
        
        instructions_raw = getattr(message_obj, 'instructions', None) # List[Union[UiParsedInstruction, UiPartiallyDecodedInstruction]]

        program_id_str_spl = str(TOKEN_PROGRAM_ID)
        program_id_str_spl2022 = str(TOKEN_2022_PROGRAM_ID)
        potential_mint: Optional[str] = None
        
        # Check outer instructions
        if isinstance(instructions_raw, list):
            for ix_obj in instructions_raw: # ix_obj is UiParsedInstruction or UiPartiallyDecodedInstruction
                prog_id_str = str(ix_obj.program_id) if hasattr(ix_obj, 'program_id') else None
                parsed_content = ix_obj.parsed if hasattr(ix_obj, 'parsed') else None # This is a dict for UiParsedInstruction
                
                if prog_id_str in (program_id_str_spl, program_id_str_spl2022) and isinstance(parsed_content, dict):
                    ix_type = parsed_content.get("type")
                    if ix_type == "initializeMint2" or ix_type == "initializeMint":
                        info_dict = parsed_content.get("info")
                        if isinstance(info_dict, dict):
                            potential_mint = info_dict.get("mint")
                            if potential_mint and isinstance(potential_mint, str):
                                logger.info(f"Found {ix_type} (outer): Mint {potential_mint} in tx {signature}"); return potential_mint
        
        # Check inner instructions if no mint found in outer
        if not potential_mint and meta_object.inner_instructions:
            for inner_group_obj in meta_object.inner_instructions: # UiInnerInstruction
                if not inner_group_obj.instructions: continue
                for ix_obj in inner_group_obj.instructions: # UiParsedInstruction or UiPartiallyDecodedInstruction
                    prog_id_str = str(ix_obj.program_id) if hasattr(ix_obj, 'program_id') else None
                    parsed_content = ix_obj.parsed if hasattr(ix_obj, 'parsed') else None
                    
                    if prog_id_str in (program_id_str_spl, program_id_str_spl2022) and isinstance(parsed_content, dict):
                        ix_type = parsed_content.get("type")
                        if ix_type == "initializeMint2" or ix_type == "initializeMint":
                            info_dict = parsed_content.get("info")
                            if isinstance(info_dict, dict):
                                potential_mint = info_dict.get("mint")
                                if potential_mint and isinstance(potential_mint, str):
                                    logger.info(f"Found {ix_type} (inner/CPI): Mint {potential_mint} in tx {signature}"); return potential_mint
        
        if not potential_mint: 
            logger.info(f"[{signature}] No InitializeMint instruction found after parsing tx details.")
        return potential_mint
        
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code if hasattr(e, 'response') and e.response else "Unknown"
        logger.warning(f"HTTP error (status {status_code}) processing tx {signature}: {e}"); return None
    except Exception as e: 
        logger.error(f"Generic error processing tx {signature}: {e}", exc_info=True); return None

def parse_rpc_logs_response_content(notification_content: RpcLogsResponse) -> Tuple[List[str], str]:
    """Extracts logs and signature from an RpcLogsResponse object."""
    logs_notif: LogsNotification = notification_content.value
    return logs_notif.logs, str(logs_notif.signature)

async def process_pending_mints_periodically(client: AsyncClient):
    """Periodically retries fetching metadata for mints that failed previously."""
    global pending_mints # Added this line
    while True:
        try:
            await asyncio.sleep(60) # Interval to check pending mints
            if not pending_mints: continue
            logger.info(f"--- Processing {len(pending_mints)} pending mints ---")
            mints_to_retry_this_round = pending_mints[:] # Create a copy to iterate over
            successful_this_round: List[str] = []
            output_file_exists = os.path.exists("minted_tokens.jsonl")

            for mint_addr in mints_to_retry_this_round:
                already_fully_processed = False
                # Check if already saved with name to avoid redundant processing
                if output_file_exists and mint_addr in processed: # Quick check via 'processed' set
                    try: # Deeper check in file for name
                        with open("minted_tokens.jsonl", "r", encoding="utf-8") as f_check:
                            for line in f_check:
                                try: data = json.loads(line)
                                except json.JSONDecodeError: continue
                                if data.get("mint") == mint_addr and data.get("name", "").strip():
                                    already_fully_processed = True; break
                    except FileNotFoundError: pass # Should be rare if output_file_exists is true

                if already_fully_processed:
                    logger.debug(f"Mint {mint_addr} already fully processed with name, removing from pending.")
                    successful_this_round.append(mint_addr)
                    continue

                logger.debug(f"Retrying metadata for pending mint: {mint_addr}")
                metadata = await fetch_metadata(client, mint_addr, retries=2, delay=3.0, is_pending_retry=True)
                if metadata and metadata.get("name", "").strip(): # Ensure name is valid
                    logger.info(f"Successfully fetched metadata for PENDING mint: {mint_addr}")
                    successful_this_round.append(mint_addr)
                    # Since metadata was successful, it's now considered processed for stats
                    # if it wasn't already counted. fetch_metadata handles the primary stats update.
                    # Here we ensure it's in the 'processed' set for future checks.
                    processed.add(mint_addr) 
                    if mint_addr not in stats.get("processed_with_metadata", set()):
                         stats["total_mints_found"] += 1 # Count if new mint with metadata
                         stats.setdefault("processed_with_metadata", set()).add(mint_addr)

                else:
                    logger.warning(f"Still failed to fetch complete metadata for PENDING mint {mint_addr}.")
            
            if successful_this_round: # Update the main pending_mints list
                pending_mints = [m for m in pending_mints if m not in successful_this_round]
            logger.info(f"--- Finished processing pending mints. {len(pending_mints)} still pending. ---")
        except asyncio.CancelledError: 
            logger.info("Pending mints processing task cancelled."); break
        except Exception as e: 
            logger.error(f"Error in process_pending_mints_periodically: {e}", exc_info=True)
            
            
async def periodic_stats_update():
    """Periodically logs and saves runtime statistics."""
    while True:
        try:
            await asyncio.sleep(300) # Log stats every 5 minutes
            logger.info(f"STATS_UPDATE: {json.dumps(stats, indent=2)}")
            with open("token_tracker_stats.json", "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)
        except asyncio.CancelledError: 
            logger.info("Stats update task cancelled."); break
        except Exception as e: 
            logger.error(f"Error updating stats: {e}", exc_info=True)

async def handle_log_notification(
    notification_content: RpcLogsResponse,
    http_client: AsyncClient,
    tx_queue: asyncio.Queue[str]
):
    """Processes a single log notification from the WebSocket stream."""
    logs, signature = parse_rpc_logs_response_content(notification_content)
    logger.debug(f"[{signature}] Received logs. Count: {len(logs)}")
    
    has_init_mint_keyword = any("InitializeMint" in log or "Initialize mint " in log for log in logs)
    if has_init_mint_keyword: # More verbose logging for potential mints
        logger.info(f"[{signature}] InitializeMint keyword found. Full logs for this transaction:")
        for i, log_entry in enumerate(logs): logger.info(f"[{signature}] Log [{i}]: {log_entry}")

    mint_addr_from_logs = find_mint_address_in_logs(logs, signature)

    if mint_addr_from_logs and mint_addr_from_logs != "TBD_BY_FULL_PARSE" and mint_addr_from_logs not in processed:
        logger.info(f"🎯 Potential mint (direct log parse): {mint_addr_from_logs} (sig: {signature})")
        processed.add(mint_addr_from_logs)
        metadata = await fetch_metadata(http_client, mint_addr_from_logs)
        if metadata: # Successfully got metadata
            if mint_addr_from_logs not in stats.get("processed_with_metadata", set()):
                stats["total_mints_found"] += 1
                stats.setdefault("processed_with_metadata", set()).add(mint_addr_from_logs)
        elif mint_addr_from_logs not in pending_mints: # If metadata fails, add to pending
            logger.warning(f"Failed initial metadata for {mint_addr_from_logs}. Adding to pending.")
            pending_mints.append(mint_addr_from_logs)
    
    elif (mint_addr_from_logs == "TBD_BY_FULL_PARSE" or (not mint_addr_from_logs and has_init_mint_keyword)) \
         and PROCESS_TRANSACTIONS_FALLBACK:
        logger.debug(f"[{signature}] Log analysis suggests mint, but not directly parsable or generic InitializeMint. Queuing for full tx processing.")
        await tx_queue.put(signature)



# Ensure these imports are present at the top of your file
import asyncio
import json
import logging
import time # For time.monotonic()
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from solders.rpc.config import RpcTransactionLogsFilterMentions, RpcTransactionLogsFilter
from solders.rpc.responses import RpcLogsResponse, LogsNotificationResult # Corrected
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed # For str(Confirmed)
from solana.rpc.websocket_api import SolanaWsClientProtocol
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK


# Assume logger and stats are defined globally as in your script
# logger = logging.getLogger("TokenTracker")
# stats: Dict[str, Any] = {}
# async def handle_log_notification(...): ... (defined elsewhere)


async def unified_subscription_consumer_task(
    ws_client: SolanaWsClientProtocol, # Still SolanaWsClientProtocol, risk of conflict remains
    filters_with_names: List[Tuple[RpcTransactionLogsFilter, str]],
    shared_http_client: AsyncClient,
    shared_tx_queue: asyncio.Queue[str]
):
    """
    Consumes messages from multiple WebSocket log subscriptions using a single receiver.
    MANUAL REQUEST/RESPONSE HANDLING APPROACH + LIST HANDLING.
    WARNING: This approach directly uses ws_client.send/recv and may conflict with
             solana-py's internal message processing loop if ws_client is
             SolanaWsClientProtocol.
    """
    logger.warning("Using MANUAL unified_subscription_consumer_task with LIST HANDLING. This is experimental.")

    subscription_ids: Dict[int, str] = {}  # Server-assigned subscription ID -> name_tag
    pending_requests: Dict[int, str] = {} # Our local request_id -> name_tag
    next_request_id = 1

    # --- Corrected sub_filter serialization ---
    def serialize_filter_param(sub_filter_obj: RpcTransactionLogsFilter) -> Union[str, Dict[str, List[str]]]:
        if isinstance(sub_filter_obj, RpcTransactionLogsFilterMentions):
            return {"mentions": [str(sub_filter_obj.pubkey)]}
        # For RpcTransactionLogsFilter.All or RpcTransactionLogsFilter.AllWithVotes
        # solders uses simple string representations for these when they are members of the enum-like class
        # Example: RpcTransactionLogsFilter.All is effectively the string "all"
        # So, str(sub_filter_obj) should work if it's not a Mentions filter.
        filter_str_value = str(sub_filter_obj)
        if filter_str_value in ("all", "allWithVotes"):
            return filter_str_value
        logger.error(f"Unsupported RpcTransactionLogsFilter type for manual serialization: {type(sub_filter_obj)}, value: {sub_filter_obj}")
        raise ValueError(f"Unsupported RpcTransactionLogsFilter type: {type(sub_filter_obj)}")

    # Send subscription requests
    for sub_filter_item, name_tag_item in filters_with_names:
        request_id = next_request_id
        next_request_id += 1

        mentions_str_log = str(sub_filter_item.pubkey) if isinstance(sub_filter_item, RpcTransactionLogsFilterMentions) else str(sub_filter_item)
        logger.info(f"[{name_tag_item}] Setting up manual subscription (Req ID: {request_id}, Filter: {mentions_str_log})")

        try:
            serialized_filter = serialize_filter_param(sub_filter_item)
            request_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "logsSubscribe",
                "params": [
                    serialized_filter,
                    {"commitment": str(Confirmed)}
                ]
            }
            request_json = json.dumps(request_payload)
            logger.debug(f"[{name_tag_item}] Sending manual request: {request_json}")
            await ws_client.send(request_json)
            pending_requests[request_id] = name_tag_item
        except Exception as e_send:
            logger.error(f"[{name_tag_item}] Error sending manual subscription request: {e_send}", exc_info=True)
            continue

    # Phase 1: Wait for subscription confirmations
    confirmation_timeout = 10.0  # seconds
    processing_start_time = time.monotonic()
    initial_pending_count = len(pending_requests)

    logger.info(f"MANUAL_LIST_HANDLER: Waiting for {initial_pending_count} subscription confirmations (timeout: {confirmation_timeout}s)...")

    while pending_requests and (time.monotonic() - processing_start_time < confirmation_timeout):
        try:
            raw_message_outer = await asyncio.wait_for(ws_client.recv(), timeout=1.0)
            logger.debug(f"CONFIRM_PHASE_RECV_OUTER: Type: {type(raw_message_outer)}, Content: {str(raw_message_outer)[:500]}...") # Log first 500 chars

            messages_to_process = [raw_message_outer] if not isinstance(raw_message_outer, list) else raw_message_outer

            for i_msg, raw_message_inner in enumerate(messages_to_process):
                logger.debug(f"CONFIRM_PHASE_RECV_INNER [{i_msg}/{len(messages_to_process)-1}]: Type: {type(raw_message_inner)}, Content: {str(raw_message_inner)[:500]}...")
                message_data: Any
                if isinstance(raw_message_inner, (str, bytes, bytearray)):
                    try:
                        message_data = json.loads(raw_message_inner)
                    except json.JSONDecodeError:
                        logger.warning(f"CONFIRM_PHASE: JSON decode error for inner message: {raw_message_inner!r}")
                        continue
                elif isinstance(raw_message_inner, dict):
                    message_data = raw_message_inner
                else:
                    logger.warning(f"CONFIRM_PHASE: Inner message of unexpected type: {type(raw_message_inner)}. Content: {str(raw_message_inner)[:200]}")
                    continue
                
                # Ensure message_data is a dict
                if not isinstance(message_data, dict):
                    logger.warning(f"CONFIRM_PHASE: Parsed inner message_data is not a dict: {message_data!r}")
                    continue

                # Check if it's a subscription confirmation
                if "id" in message_data and "result" in message_data:
                    req_id = message_data.get("id")
                    server_sub_id = message_data.get("result")

                    if isinstance(req_id, int) and req_id in pending_requests:
                        name_tag_confirmed = pending_requests.pop(req_id)
                        if isinstance(server_sub_id, int):
                            subscription_ids[server_sub_id] = name_tag_confirmed
                            stats["ws_subscriptions_active"] = len(subscription_ids) # Update active count
                            logger.info(f"[{name_tag_confirmed}] Manual subscription CONFIRMED. Req ID: {req_id}, Server Sub ID: {server_sub_id}")
                        else:
                            logger.error(f"[{name_tag_confirmed}] Manual subscription response for Req ID {req_id} has invalid Server Sub ID: {server_sub_id!r}")
                    # else: might be a response to an unsubscribe request, or late/duplicate
                elif "id" in message_data and "error" in message_data:
                    req_id = message_data.get("id")
                    error_details = message_data.get("error")
                    if isinstance(req_id, int) and req_id in pending_requests:
                        name_tag_error = pending_requests.pop(req_id)
                        logger.error(f"[{name_tag_error}] Manual subscription FAILED for Req ID {req_id}. Error: {error_details!r}")
                elif message_data.get("method") == "logsNotification":
                     logger.debug(f"CONFIRM_PHASE: Received a logsNotification (will be processed in main loop if sub is confirmed): {str(message_data)[:200]}...")
                # else: other message types
            
        except asyncio.TimeoutError:
            logger.debug("CONFIRM_PHASE: asyncio.TimeoutError on ws_client.recv(), continuing confirmation wait.")
            continue
        except json.JSONDecodeError: # If raw_message_outer itself was a string that failed to parse before list check
            logger.warning(f"CONFIRM_PHASE: JSON decode error for outer message: {raw_message_outer!r}")
        except ConnectionClosedError as e:
            logger.error(f"CONFIRM_PHASE: Connection closed: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"CONFIRM_PHASE: Error processing message: {e}", exc_info=True)

    if pending_requests:
        for req_id_pending, name_tag_p in pending_requests.items():
            logger.error(f"[{name_tag_p}] Manual subscription TIMEOUT for Req ID {req_id_pending}. Never received confirmation.")
    
    if not subscription_ids:
        logger.error("MANUAL_LIST_HANDLER: No subscriptions were successfully established. Exiting consumer task.")
        stats["ws_subscriptions_active"] = 0
        return

    logger.info(f"MANUAL_LIST_HANDLER: Successfully established {len(subscription_ids)} subscriptions. Starting main notification processing. Active Server IDs: {list(subscription_ids.keys())}")
    
    # Phase 2: Main loop for processing notifications
    try:
        while True:
            raw_message_outer_notif = await ws_client.recv()
            logger.debug(f"NOTIF_PHASE_RECV_OUTER: Type: {type(raw_message_outer_notif)}, Content: {str(raw_message_outer_notif)[:500]}...")

            messages_to_process_notif = [raw_message_outer_notif] if not isinstance(raw_message_outer_notif, list) else raw_message_outer_notif

            for i_msg_n, raw_message_inner_notif in enumerate(messages_to_process_notif):
                logger.debug(f"NOTIF_PHASE_RECV_INNER [{i_msg_n}/{len(messages_to_process_notif)-1}]: Type: {type(raw_message_inner_notif)}, Content: {str(raw_message_inner_notif)[:500]}...")
                notification_data: Any
                try:
                    if isinstance(raw_message_inner_notif, (str, bytes, bytearray)):
                        try:
                            notification_data = json.loads(raw_message_inner_notif)
                        except json.JSONDecodeError:
                            logger.warning(f"NOTIF_PHASE: JSON decode error for inner message: {raw_message_inner_notif!r}")
                            continue
                    elif isinstance(raw_message_inner_notif, dict):
                        notification_data = raw_message_inner_notif
                    else:
                        logger.warning(f"NOTIF_PHASE: Inner message of unexpected type: {type(raw_message_inner_notif)}. Content: {str(raw_message_inner_notif)[:200]}")
                        continue
                    
                    if not isinstance(notification_data, dict):
                        logger.warning(f"NOTIF_PHASE: Parsed inner notification_data is not a dict: {notification_data!r}")
                        continue

                    if notification_data.get("method") == "logsNotification" and \
                       "params" in notification_data and isinstance(notification_data["params"], dict) and \
                       "result" in notification_data["params"] and isinstance(notification_data["params"]["result"], dict) and \
                       "subscription" in notification_data["params"]:
                        
                        server_sub_id_notif = notification_data["params"].get("subscription")
                        
                        if isinstance(server_sub_id_notif, int) and server_sub_id_notif in subscription_ids:
                            name_tag_notif = subscription_ids[server_sub_id_notif]
                            
                            logs_payload_wrapper = notification_data["params"]["result"]
                            actual_logs_payload_dict = logs_payload_wrapper.get("value")

                            if isinstance(actual_logs_payload_dict, dict) and \
                               "signature" in actual_logs_payload_dict and \
                               "logs" in actual_logs_payload_dict:
                                
                                logs_notif_result_obj = LogsNotificationResult.from_dict(actual_logs_payload_dict)
                                logs_response = RpcLogsResponse(value=logs_notif_result_obj)
                                
                                logger.debug(f"[{name_tag_notif}] Received log notification for Server Sub ID {server_sub_id_notif}")
                                await handle_log_notification(logs_response, shared_http_client, shared_tx_queue)
                            else:
                                logger.warning(f"[{name_tag_notif}] Malformed logs 'value' payload for Server Sub ID {server_sub_id_notif}: {logs_payload_wrapper!r}")
                        # else: untracked subscription ID
                    # else: other message types, e.g., late confirmations, errors
                except json.JSONDecodeError: # If inner message was string and failed
                     logger.warning(f"NOTIF_PHASE: JSON decode error for inner message (outer try): {raw_message_inner_notif!r}")
                except Exception as e_proc:
                    logger.error(f"NOTIF_PHASE: Error processing inner notification: {e_proc}. Data: {raw_message_inner_notif!r}", exc_info=True)
    
    except ConnectionClosedOK:
        logger.info(f"MANUAL_LIST_HANDLER: Connection closed OK.")
        raise 
    except ConnectionClosedError as e:
        logger.warning(f"MANUAL_LIST_HANDLER: Connection closed with error: {e.code} {e.reason}")
        raise 
    except asyncio.CancelledError:
        logger.info("MANUAL_LIST_HANDLER: Task cancelled.")
        raise
    except Exception as e_main_loop:
        logger.error(f"MANUAL_LIST_HANDLER: Unhandled error in main notification loop: {e_main_loop}", exc_info=True)
        raise
    finally:
        # ... (keep your existing finally block for unsubscription and clearing stats) ...
        active_server_sub_ids = list(subscription_ids.keys())
        logger.info(f"MANUAL_LIST_HANDLER: unified_subscription_consumer_task finishing. Active Server Sub IDs before unsub: {active_server_sub_ids}")
        
        if ws_client.is_connected() and active_server_sub_ids: # Check ws_client.is_connected()
            logger.info(f"MANUAL_LIST_HANDLER: Attempting to unsubscribe from {len(active_server_sub_ids)} subscriptions.")
            for server_sub_id_to_unsub in active_server_sub_ids:
                unsub_request_id = next_request_id
                next_request_id +=1
                name_tag_unsub = subscription_ids.get(server_sub_id_to_unsub, "UnknownSubToUnsub")
                try:
                    unsub_payload = {
                        "jsonrpc": "2.0",
                        "id": unsub_request_id,
                        "method": "logsUnsubscribe",
                        "params": [server_sub_id_to_unsub]
                    }
                    unsub_json = json.dumps(unsub_payload)
                    logger.debug(f"[{name_tag_unsub}] Sending manual unsubscribe for Server Sub ID {server_sub_id_to_unsub}: {unsub_json}")
                    await ws_client.send(unsub_json)
                except Exception as e_unsub:
                    logger.warning(f"[{name_tag_unsub}] Error sending manual unsubscribe for Server Sub ID {server_sub_id_to_unsub}: {e_unsub}")
        
        stats["ws_subscriptions_active"] = 0 
        subscription_ids.clear()
        pending_requests.clear()
        logger.info(f"MANUAL_LIST_HANDLER: Unified subscription consumer task finished and attempted cleanup.")
        
async def listen_once(shutdown_signal: asyncio.Event) -> None:
    """Manages a single WebSocket session, including setting up subscriptions and background tasks."""
    http_client = AsyncClient(HTTP_RPC, timeout=httpx.Timeout(10.0, read=30.0))
    
    # Background tasks for this session
    session_tasks = [
        asyncio.create_task(process_transaction_from_queue(http_client), name="TransactionWorker"),
        asyncio.create_task(process_pending_mints_periodically(http_client), name="PendingMintsProcessor"),
        asyncio.create_task(periodic_stats_update(), name="StatsUpdater")
    ]
    
    try:
        async with connect(WS_URI, ping_interval=20, ping_timeout=10) as ws_client:
            logger.info("[+] WebSocket client connected. Setting up subscriptions...")

            spl_token_filter = RpcTransactionLogsFilterMentions(TOKEN_PROGRAM_ID)
            token_2022_filter = RpcTransactionLogsFilterMentions(TOKEN_2022_PROGRAM_ID)

            # Define filters with their respective names
            subscription_filters = [
                (spl_token_filter, "SPL_TOKEN"),
                (token_2022_filter, "TOKEN_2022")
            ]

            # Create a single unified consumer task for all subscriptions
            unified_consumer = asyncio.create_task(
                unified_subscription_consumer_task(
                    ws_client, subscription_filters, http_client, transaction_queue
                ), 
                name="UnifiedSubscriptionConsumer"
            )
            session_tasks.append(unified_consumer)
            
            # Monitor consumer task and shutdown signal
            tasks_to_monitor = [unified_consumer, asyncio.create_task(shutdown_signal.wait(), name="ShutdownWatcherListenOnce")]
            done, pending = await asyncio.wait(tasks_to_monitor, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                task_name = task.get_name() if hasattr(task, 'get_name') else "UnknownTask"
                if task_name == "ShutdownWatcherListenOnce":
                    logger.info(f"Shutdown signal received in listen_once via {task_name}. Cancelling consumer task.")
                    for p_task in pending:
                        if not p_task.done(): p_task.cancel()
                else:
                    logger.warning(f"Consumer task {task_name} finished unexpectedly.")
                    try: 
                        task.result()
                    except (ConnectionClosedError, ConnectionClosedOK, asyncio.TimeoutError, asyncio.CancelledError, RuntimeError) as e_conn_runtime:
                        logger.warning(f"Consumer {task_name} ended: {type(e_conn_runtime).__name__} - {e_conn_runtime}")
                    except Exception as e_task_unhandled:
                        logger.error(f"Consumer {task_name} died with unhandled error: {e_task_unhandled}", exc_info=True)
                    
                    # Ensure other tasks are cancelled before listen_once exits
                    for p_task in pending:
                        if not p_task.done(): p_task.cancel()
                    # Re-raise a generic error to ensure main loop attempts reconnection
                    raise RuntimeError(f"Consumer task {task_name} exited, triggering session restart.")
            
            # Await any pending consumer tasks that were cancelled to allow them to clean up
            await asyncio.gather(*[p_task for p_task in pending if not p_task.done()], return_exceptions=True)

    except (ConnectionClosedError, ConnectionClosedOK, asyncio.TimeoutError, RuntimeError) as e_ws_connect:
        logger.error(f"listen_once connection/runtime error: {type(e_ws_connect).__name__} - {e_ws_connect}", exc_info=False)
        raise
    except Exception as e_outer:
        logger.error(f"Outer error in listen_once: {type(e_outer).__name__} - {e_outer}", exc_info=True)
        raise
    finally:
        logger.info("listen_once finishing. Cleaning up all session tasks...")
        for task in session_tasks:
            if task and not task.done():
                task.cancel()
        # Await all session tasks to ensure they are properly cancelled/finished
        results = await asyncio.gather(*session_tasks, return_exceptions=True)
        for i, result in enumerate(results):
            task_name = session_tasks[i].get_name() if hasattr(session_tasks[i], 'get_name') else f"Task-{i}"
            if isinstance(result, asyncio.CancelledError):
                logger.info(f"Session task {task_name} successfully cancelled.")
            elif isinstance(result, Exception):
                logger.error(f"Error during session task {task_name} cleanup: {result}", exc_info=False)
        
        await http_client.close()
        logger.info("HTTP client closed.")

# ────────────────────── MAIN RECONNECT LOOP ──────────────────────
async def main():
    """Main function to run the WebSocket listener with a reconnection loop."""
    shutdown_event = asyncio.Event()
    # active_tasks is used to track the main listen_once task for signal handling
    active_tasks: Set[asyncio.Task[Any]] = set() 

    def handle_signal_main(sig: Any, frame: Any) -> None:
        logger.info(f"\n[×] Received signal {signal.Signals(sig).name}. Initiating graceful shutdown...")
        if not shutdown_event.is_set():
            shutdown_event.set()
            # Cancel tasks that are being managed by this main loop (primarily current_listen_task)
            for task in list(active_tasks): # Iterate over a copy
                if task and not task.done(): 
                    logger.debug(f"Cancelling task from main signal handler: {task.get_name()}")
                    task.cancel()
        else: 
            logger.warning("Shutdown already in progress.")

    signal.signal(signal.SIGINT, handle_signal_main)
    signal.signal(signal.SIGTERM, handle_signal_main)

    current_listen_task: Optional[asyncio.Task[Any]] = None
    while not shutdown_event.is_set():
        try:
            logger.info("Starting WebSocket listener session (listen_once)...")
            current_listen_task = asyncio.create_task(listen_once(shutdown_event), name="ListenOnceSession")
            active_tasks.add(current_listen_task)
            await current_listen_task # This will run until listen_once exits or is cancelled

            # If listen_once completes without error and shutdown is not set, it's unexpected.
            if not shutdown_event.is_set():
                 logger.warning("listen_once completed without error or shutdown signal. This is unexpected. Reconnecting after delay.")
                 raise RuntimeError("listen_once exited prematurely without external signal or error.")

        except asyncio.CancelledError: # This catches cancellation of main() or current_listen_task by signal
            logger.info("Main task or listen_once session was cancelled. Preparing to exit main loop.")
            if not shutdown_event.is_set(): shutdown_event.set() # Ensure event is set for other parts
            break # Exit the while loop
        except (ConnectionClosedError, ConnectionClosedOK, asyncio.TimeoutError, RuntimeError) as e: # Catch errors from listen_once
            if shutdown_event.is_set(): # If shutdown was already triggered, just exit
                logger.info("Shutdown initiated during error handling in main loop. Exiting.")
                break
            stats["reconnections"] += 1
            logger.error(f"[!] Listener session error: {type(e).__name__} - {e}. Reconnecting in 10 seconds.")
            
            reconnect_delay_task = asyncio.create_task(asyncio.sleep(10), name="ReconnectDelay")
            # Also monitor shutdown_event during this sleep
            shutdown_during_sleep_task = asyncio.create_task(shutdown_event.wait(), name="ShutdownDuringReconnectSleep")
            
            done, pending = await asyncio.wait(
                {reconnect_delay_task, shutdown_during_sleep_task}, 
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_during_sleep_task in done: # Shutdown signal came during sleep
                logger.info("Reconnect delay interrupted by shutdown signal.")
                if reconnect_delay_task in pending: reconnect_delay_task.cancel()
                break # Exit main loop
            # else, sleep completed, cancel the shutdown_watcher for this sleep
            if shutdown_during_sleep_task in pending: shutdown_during_sleep_task.cancel()
            # Loop continues for reconnection attempt

        except Exception as e_main_loop: # Catch any other truly unexpected errors
            if shutdown_event.is_set():
                logger.info("Shutdown initiated during another exception. Exiting main loop.")
                break
            logger.critical(f"[!!!] Unhandled exception in main loop (from listen_once): {type(e_main_loop).__name__} - {e_main_loop}", exc_info=True)
            logger.info("Attempting reconnect after critical error in 15s.")
            # Similar logic for shutdown check during sleep
            critical_error_delay_task = asyncio.create_task(asyncio.sleep(15), name="CriticalErrorReconnectDelay")
            shutdown_during_critical_sleep_task = asyncio.create_task(shutdown_event.wait(), name="ShutdownDuringCriticalSleep")
            done, pending = await asyncio.wait(
                {critical_error_delay_task, shutdown_during_critical_sleep_task},
                return_when=asyncio.FIRST_COMPLETED
            )
            if shutdown_during_critical_sleep_task in done:
                logger.info("Critical error reconnect delay interrupted by shutdown signal.")
                if critical_error_delay_task in pending: critical_error_delay_task.cancel()
                break
            if shutdown_during_critical_sleep_task in pending: shutdown_during_critical_sleep_task.cancel()
        finally:
            # Clean up current_listen_task from active_tasks set after it's done or cancelled
            if current_listen_task and current_listen_task in active_tasks:
                active_tasks.remove(current_listen_task)
            # Ensure the task is truly finished or cancelled
            if current_listen_task and not current_listen_task.done():
                logger.info(f"Ensuring {current_listen_task.get_name()} task is fully cancelled in main's finally block.")
                current_listen_task.cancel()
                try: 
                    await current_listen_task # Give it a chance to process cancellation
                except asyncio.CancelledError: 
                    logger.info(f"{current_listen_task.get_name()} confirmed cancelled in main finally.")
                except Exception as e_final_await: 
                    logger.error(f"Error during final await of {current_listen_task.get_name()}: {e_final_await}", exc_info=False)

    logger.info("\n[×] Main loop ended. Finalizing cleanup of all tasks...")
    # This is a last resort cleanup. Ideally, tasks are managed within their scopes (e.g., listen_once).
    remaining_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    if remaining_tasks:
        logger.info(f"Cancelling {len(remaining_tasks)} lingering tasks during final shutdown...")
        for task in remaining_tasks: 
            task.cancel()
        # Await them to allow cleanup, ignoring errors as we are shutting down.
        await asyncio.gather(*remaining_tasks, return_exceptions=True)
        logger.info("Lingering tasks processing complete.")
    
    logger.info("Saving final stats...")
    try:
        with open("token_tracker_stats.json", "w", encoding="utf-8") as f: 
            json.dump(stats, f, indent=2)
        logger.info("Final stats saved successfully.")
    except Exception as e_stat_save: 
        logger.error(f"Failed to save final stats: {e_stat_save}")
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    if os.name == 'nt': # For Windows, ProactorEventLoop can be better for signals with asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        # This KeyboardInterrupt should ideally be caught by the signal handler if signals are working.
        # If it reaches here, it means the signal handler might not have fully intercepted or
        # asyncio.run itself was interrupted before the loop started properly.
        logger.info("\n[×] Application terminated by KeyboardInterrupt directly in asyncio.run.")
    except Exception as e_top_level: 
        logger.critical(f"Unhandled top-level exception: {e_top_level}", exc_info=True)