import requests
import json
import time
from datetime import datetime, timezone, time as dt_time
from typing import Tuple, Dict, Any, List, Optional
from urllib.parse import urlencode
import os # For .env
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- Configuration ---
DASHBOARD_ID = os.getenv("DATADOG_DASHBOARD_ID", "339e0590-c5d4-11ed-9c7b-da7ad0900005-9a1e3a7c98f885976f7b54dbf67b9a3f")

WIDGET_IDS = {
    "successful_requests": os.getenv("DATADOG_WIDGET_ID_SUCCESSFUL_REQUESTS", "1726266930007018"),
    "method_counts": os.getenv("DATADOG_WIDGET_ID_METHOD_COUNTS", "8933624082545960"), # Renamed for clarity
}

# Static template variables from your URL (can also be moved to .env if they change)
STATIC_TPL_VARS = {
    "tpl_var_apiKey[0]": os.getenv("DATADOG_TPL_VAR_APIKEY", "4edf131a-6d04-4123-a65d-e0ed03e63029"),
    "tpl_var_method[0]": "*", # Usually kept as * for overall metrics
    "tpl_var_network[0]": os.getenv("DATADOG_TPL_VAR_NETWORK", "mainnet"),
    "tpl_var_projectId[0]": os.getenv("DATADOG_TPL_VAR_PROJECTID", "a0311864-9458-4725-ac41-b1dfb88aa15a")
}

OTHER_STATIC_PARAMS = {
    "fromUser": "false",
}
BASE_API_URL = f"https://p.us5.datadoghq.com/dashboard/shared_widget_update/{DASHBOARD_ID}"
REQUEST_INTERVAL_SECONDS = 1 # Be mindful of rate limits if fetching frequently or many widgets

# --- Helper Functions ---
def get_today_so_far_timestamps_ms() -> Tuple[int, int]:
    now_utc = datetime.now(timezone.utc)
    start_of_today_utc = datetime.combine(now_utc.date(), dt_time.min, tzinfo=timezone.utc)
    from_ts_ms = int(start_of_today_utc.timestamp() * 1000)
    to_ts_ms = int(now_utc.timestamp() * 1000)
    return from_ts_ms, to_ts_ms

def parse_numeric_value(val_str: Optional[str]) -> int:
    if val_str is None:
        return 0
    val_str = str(val_str).strip().lower()
    if not val_str or val_str == "-" or "no data" in val_str or "loading" in val_str:
        return 0
    
    val_str = val_str.replace(",", "")
    multiplier = 1
    
    if val_str.endswith('k'):
        num_part = val_str[:-1]
        multiplier = 1000
    elif val_str.endswith('m'):
        num_part = val_str[:-1]
        multiplier = 1_000_000
    elif val_str.endswith('b'):
        num_part = val_str[:-1]
        multiplier = 1_000_000_000
    else:
        num_part = val_str
        
    try:
        return int(float(num_part) * multiplier)
    except ValueError:
        print(f"  [Parse Warning] Could not parse value '{val_str}' into a number.")
        return 0

def parse_widget_json(widget_name_key: str, widget_id: str, response_json: Dict[str, Any]) -> Any:
    """
    Parses the relevant value from the widget's JSON response.
    """
    print(f"\n--- Parsing JSON for {widget_name_key} (ID: {widget_id}) ---")
    # print(json.dumps(response_json, indent=2)) # Keep this for deep debugging if needed

    widget_data_list = response_json.get("responses", {}).get(widget_id)
    if not widget_data_list or not isinstance(widget_data_list, list) or not widget_data_list[0]:
        print(f"  ERROR: Could not find data for widget_id '{widget_id}' in response_json['responses'].")
        return None

    actual_widget_response = widget_data_list[0]

    if widget_name_key == "successful_requests":
        # Expected structure: Query Value widget
        # Try common patterns for the single value
        val_to_parse = None
        if "value" in actual_widget_response and actual_widget_response["value"] is not None:
            val_to_parse = actual_widget_response["value"]
        elif actual_widget_response.get("attributes", {}).get("last_value") is not None:
            val_to_parse = actual_widget_response["attributes"]["last_value"]
        # Add more patterns if needed based on actual JSON for this widget
        
        if val_to_parse is not None:
            print(f"  Found value for successful_requests: {val_to_parse}")
            return parse_numeric_value(str(val_to_parse))
        else:
            print(f"  ERROR: Could not find known value key for 'successful_requests' in widget {widget_id}. Full widget JSON:")
            print(json.dumps(actual_widget_response, indent=2))
            return 0 # Default to 0 if not found

    elif widget_name_key == "method_counts":
        # Expected structure: Toplist from your example
        try:
            inputs_list = actual_widget_response.get("data", [{}])[0].get("attributes", {}).get("inputs", [])
            methods_data = {}
            if inputs_list:
                for item in inputs_list:
                    method_name_full = item.get("metadata", {}).get("groupTags", [""])[0]
                    method_name_parts = method_name_full.split(":")
                    method_name = method_name_parts[-1] if len(method_name_parts) > 1 else method_name_full
                    
                    value_raw = item.get("value")
                    value = parse_numeric_value(str(value_raw) if value_raw is not None else "0") # Parse value here

                    if method_name:
                        methods_data[method_name] = value
                print(f"  Parsed method_counts: {methods_data}")
                return methods_data
            else:
                print(f"  ERROR: 'inputs' list not found or empty for 'method_counts' in widget {widget_id}.")
                return {}
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            print(f"  ERROR: Parsing 'method_counts' for widget {widget_id}: {e}. Full widget JSON:")
            print(json.dumps(actual_widget_response, indent=2))
            return {}
            
    print(f"  WARNING: No specific parser for widget_name_key '{widget_name_key}'. Returning None.")
    return None


def fetch_widget_data(widget_name_key: str, widget_id: str) -> Optional[Any]:
    if not widget_id or "YOUR_" in widget_id.upper():
        print(f"Skipping {widget_name_key} due to placeholder WIDGET_ID: {widget_id}")
        return None

    from_ts, to_ts = get_today_so_far_timestamps_ms()
    
    # Parameters common to most shared_widget_update calls
    # Specific widgets might ignore some or require others, check Network tab if issues arise
    params = {
        **STATIC_TPL_VARS,
        **OTHER_STATIC_PARAMS,
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "live": "false", 
        "include_events": "false", 
        "no_graph": "true" # Often good for getting summary data for toplists/query_values
    }
    
    endpoint_url = f"{BASE_API_URL}/{widget_id}" 

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest", # Good practice for these types of endpoints
        "DNT": "1" 
    }

    try:
        print(f"Fetching data for: {widget_name_key} (ID: {widget_id})")
        # For debugging, print the exact URL that will be called
        # req = requests.Request('GET', endpoint_url, params=params, headers=headers)
        # prepared = req.prepare()
        # print(f"  Full Request URL: {prepared.url}") 
        
        response = requests.get(endpoint_url, params=params, headers=headers, timeout=30)
        print(f"  Status Code: {response.status_code}")
        
        response.raise_for_status()
        
        # Optional: Print rate limit headers
        # print(f"  RateLimit-Limit: {response.headers.get('x-ratelimit-limit')}")
        # print(f"  RateLimit-Remaining: {response.headers.get('x-ratelimit-remaining')}")
        # print(f"  RateLimit-Reset: {response.headers.get('x-ratelimit-reset')}")
        
        response_json = response.json()
        return parse_widget_json(widget_name_key, widget_id, response_json)
        
    except requests.exceptions.HTTPError as http_err:
        print(f"  HTTP error occurred for {widget_name_key}: {http_err}")
        try:
            print(f"  Response content: {response.text[:500]}")
        except:
            pass
    except requests.exceptions.RequestException as req_err:
        print(f"  Request error occurred for {widget_name_key}: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"  JSON decode error for {widget_name_key}: {json_err}")
        try:
            print(f"  Response content: {response.text[:500]}")
        except:
            pass
    except Exception as e:
        print(f"  An unexpected error occurred for {widget_name_key}: {e}")
        import traceback
        traceback.print_exc()
    
    return None

def main():
    helius_usage = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_successful_requests": 0,
        "method_counts": {}
    }

    # Fetch "Successful Requests"
    sr_data = fetch_widget_data("successful_requests", WIDGET_IDS["successful_requests"])
    if sr_data is not None:
        helius_usage["total_successful_requests"] = sr_data
    
    time.sleep(REQUEST_INTERVAL_SECONDS) # Respect rate limits

    # Fetch "Method Counts" (which includes balances and transactionhistory)
    mc_data = fetch_widget_data("method_counts", WIDGET_IDS["method_counts"])
    if isinstance(mc_data, dict):
        helius_usage["method_counts"] = mc_data
        # If "total_successful_requests" was not found or was 0, 
        # and we have method counts, we can sum them as a fallback.
        # This assumes 'successful_requests' widget should ideally match the sum from 'method_counts'.
        if helius_usage["total_successful_requests"] == 0 and mc_data:
            print("  'total_successful_requests' was 0, calculating sum from method_counts as a fallback.")
            helius_usage["total_successful_requests"] = sum(mc_data.values())


    print("\n--- Aggregated Helius Usage Data ---")
    print(json.dumps(helius_usage, indent=2))

    with open("datadog_helius_usage.json", "w") as f:
        json.dump(helius_usage, f, indent=2)
    print("\nFinal data saved to datadog_helius_usage.json")

if __name__ == "__main__":
    if "YOUR_" in WIDGET_IDS["successful_requests"].upper() or \
       "YOUR_" in WIDGET_IDS["method_counts"].upper():
        print("ERROR: Placeholder WIDGET_IDS detected. Please update the script with actual IDs from your browser's DevTools.")
    else:
        main()