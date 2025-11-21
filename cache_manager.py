# --- START OF FILE cache_manager.py ---

import sqlite3
import time
import json
import os       # Added for makedirs
import random   # Added for retry delay
from typing import Optional, Any

DB_FILE = 'api_cache.db'
DEFAULT_MAX_AGE_SECONDS = 7 * 24 * 60 * 60 # 7 days

# --- Retry Configuration ---
MAX_RETRIES = 5 # Number of times to retry on lock
RETRY_MIN_DELAY = 0.05 # Minimum seconds to wait
RETRY_MAX_DELAY = 0.25 # Maximum seconds to wait (Increased slightly)
# --- End Retry Config ---

# --- Ensure DB Directory Exists ---
db_dir = os.path.dirname(DB_FILE)
if db_dir and not os.path.exists(db_dir):
    try:
        os.makedirs(db_dir, exist_ok=True)
        print(f"[Cache] Created directory for database: {db_dir}")
    except OSError as e:
        print(f"[Cache Critical] Failed to create directory {db_dir}: {e}")
        # Decide if you want to exit or continue if dir creation fails
        # exit()
# --- End Directory Check ---


def _get_connection() -> sqlite3.Connection:
    """
    Gets a connection to the SQLite database, enables WAL mode,
    and sets a reasonable timeout.
    """
    try:
        # Set a timeout (in seconds) for how long to wait if the DB is locked
        conn = sqlite3.connect(DB_FILE, timeout=10.0) # e.g., wait up to 10 seconds

        # --- Enable WAL Mode ---
        try:
            # Execute PRAGMA statements using execute method for safety
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;") # Often recommended with WAL
            # print("[Cache] WAL mode enabled.") # Optional: Log successful WAL mode set
        except sqlite3.Error as e:
            # If WAL mode fails (e.g., read-only filesystem), it's not critical but good to know
            print(f"[Cache Warning] Could not enable WAL mode or set synchronous=NORMAL: {e}. Performance might be impacted under heavy load.")
        # --- END WAL Mode ---
        return conn
    except sqlite3.Error as e:
        print(f"[Cache Critical] Error connecting to database {DB_FILE}: {e}")
        raise # Re-raise critical connection errors


def _execute_with_retry(conn: sqlite3.Connection, query: str, params: tuple = ()) -> sqlite3.Cursor:
    """Executes a query with retry logic for 'database is locked' errors."""
    cursor = conn.cursor()
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            cursor.execute(query, params)
            return cursor # Return cursor on success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                last_exception = e
                # Use exponential backoff with jitter for retries
                wait_time = (random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * (2 ** attempt))
                # Cap wait time to avoid excessively long waits
                wait_time = min(wait_time, 2.0) # Cap wait at 2 seconds max
                print(f"[Cache Lock] DB locked on execute attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time:.3f}s...")
                time.sleep(wait_time)
            else:
                # Different operational error, raise it immediately
                print(f"[Cache Error] Non-lock OperationalError during execute: {e}")
                raise e
        except sqlite3.Error as e:
            # Other SQLite errors
            print(f"[Cache Error] SQLite error during execute: {e}")
            raise e

    # If loop finishes without success
    print(f"[Cache Error] Failed to execute query after {MAX_RETRIES} retries due to lock: {query[:50]}...")
    if last_exception:
        raise last_exception # Raise the last lock error encountered
    else:
        # Should not happen unless MAX_RETRIES is 0 or loop is broken unexpectedly
        raise sqlite3.OperationalError(f"Failed to execute query after {MAX_RETRIES} retries (unknown reason)")


def _commit_with_retry(conn: sqlite3.Connection):
    """Commits changes with retry logic for 'database is locked' errors."""
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            conn.commit()
            return # Success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                last_exception = e
                wait_time = (random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * (2 ** attempt))
                wait_time = min(wait_time, 2.0) # Cap wait
                print(f"[Cache Lock] DB locked on commit attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time:.3f}s...")
                time.sleep(wait_time)
            else:
                print(f"[Cache Error] Non-lock OperationalError during commit: {e}")
                raise e
        except sqlite3.Error as e:
            print(f"[Cache Error] SQLite error during commit: {e}")
            raise e

    # If loop finishes without success
    print(f"[Cache Error] Failed to commit after {MAX_RETRIES} retries due to lock.")
    if last_exception:
        raise last_exception
    else:
        raise sqlite3.OperationalError(f"Failed to commit after {MAX_RETRIES} retries (unknown reason)")


def init_db():
    """Initializes the SQLite database and cache table."""
    print("[Cache] Initializing cache database...")
    conn = None
    try:
        conn = _get_connection() # Gets connection with WAL enabled
        _execute_with_retry(conn, '''
            CREATE TABLE IF NOT EXISTS api_cache (
                cache_key TEXT PRIMARY KEY,
                response_json TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )
        ''')
        _execute_with_retry(conn, '''
            CREATE INDEX IF NOT EXISTS idx_cache_key ON api_cache (cache_key);
        ''')
        # Indexing timestamp might be useful for cleanup performance
        _execute_with_retry(conn, '''
            CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON api_cache (timestamp);
        ''')
        _commit_with_retry(conn) # Commit schema changes
        print("[Cache] Database initialized successfully.")
    except sqlite3.Error as e:
        print(f"[Cache Error] Error during DB initialization: {e}")
    finally:
        if conn:
            conn.close()


def set_cache(key: str, data: Any):
    """Stores data in the cache with retry logic."""
    if not key or data is None:
        return
    conn = None
    try:
        conn = _get_connection()
        current_time = int(time.time())
        data_str = json.dumps(data) # Serialize data to JSON string first

        query = "INSERT OR REPLACE INTO api_cache (cache_key, response_json, timestamp) VALUES (?, ?, ?)"
        _execute_with_retry(conn, query, (key, data_str, current_time)) # Execute insert/replace
        _commit_with_retry(conn) # Commit the change

        # print(f"[Cache Set] Key: {key[:50]}...") # Keep verbose logging commented unless debugging
    except sqlite3.Error as e:
        # Errors from retry functions will be raised, but catch others here
        print(f"[Cache Error] Failed to set cache for key {key[:50]}...: {e}")
    except TypeError as e:
         print(f"[Cache Error] Failed to serialize data for key {key[:50]}...: {e}")
    finally:
        if conn:
            conn.close()


def get_cache(key: str, max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS) -> Optional[Any]:
    """Retrieves data from cache if not expired, with retry logic."""
    if not key:
        return None
    conn = None
    result = None
    try:
        conn = _get_connection()
        current_time = int(time.time())
        min_valid_timestamp = current_time - max_age_seconds

        query = "SELECT response_json, timestamp FROM api_cache WHERE cache_key = ? AND timestamp >= ?"
        cursor = _execute_with_retry(conn, query, (key, min_valid_timestamp)) # Use retry wrapper
        row = cursor.fetchone()

        if row:
            response_json_str = row[0]
            # Deserialize JSON string back to Python object
            result = json.loads(response_json_str)
            # print(f"[Cache Hit] Key: {key[:50]}...") # Keep verbose logging commented unless debugging
        # else:
            # print(f"[Cache Miss] Key: {key[:50]}...") # Keep verbose logging commented unless debugging

    except sqlite3.Error as e:
        print(f"[Cache Error] Failed to get cache for key {key[:50]}...: {e}")
    except json.JSONDecodeError as e:
         print(f"[Cache Error] Failed to decode cached JSON for key {key[:50]}...: {e}")
    finally:
        if conn:
            conn.close()

    return result


def cleanup_cache(max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS):
    """Removes expired entries from the cache, with retry logic."""
    conn = None
    deleted_count = 0
    try:
        conn = _get_connection()
        expiration_time = int(time.time()) - max_age_seconds
        query = "DELETE FROM api_cache WHERE timestamp < ?"

        cursor = _execute_with_retry(conn, query, (expiration_time,)) # Use retry wrapper
        deleted_count = cursor.rowcount # Get count *before* committing
        _commit_with_retry(conn) # Commit the delete

        if deleted_count > 0:
            print(f"[Cache Cleanup] Removed {deleted_count} expired cache entries older than {max_age_seconds // (24*60*60)} days.")
        # else:
        #     print("[Cache Cleanup] No expired entries found.")

        # Optional: Vacuum to reclaim space. Can be slow and cause locks itself.
        # print("[Cache Cleanup] Vacuuming database...")
        # _execute_with_retry(conn, "VACUUM;")
        # _commit_with_retry(conn)
        # print("[Cache Cleanup] Vacuum complete.")

    except sqlite3.Error as e:
        print(f"[Cache Error] Failed during cache cleanup: {e}")
    finally:
        if conn:
            conn.close()

# --- END OF FILE cache_manager.py ---