import requests
import json
import time
from datetime import datetime, timezone, timedelta, time as dt_time
from typing import Tuple, Dict, Any, List, Optional
from urllib.parse import urlencode
import os
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

DASHBOARD_ID = "339e0590-c5d4-11ed-9c7b-da7ad0900005-9a1e3a7c98f885976f7b54dbf67b9a3f"

WIDGET_IDS = {
    "successful_requests": os.getenv("DATADOG_WIDGET_ID_SUCCESSFUL_REQUESTS", "1726266930007018"),
    "method_breakdown": os.getenv("DATADOG_WIDGET_ID_METHOD_BREAKDOWN", "8933624082545960"),
}

STATIC_TPL_VARS = {
    "tpl_var_apiKey[0]": "4edf131a-6d04-4123-a65d-e0ed03e63029",
    "tpl_var_method[0]": "*",
    "tpl_var_network[0]": "mainnet",
    "tpl_var_projectId[0]": "a0311864-9458-4725-ac41-b1dfb88aa15a"
}

# These are generally for XHR requests originating from the dashboard page itself.
# 'live' and time-related params will be overridden by TIME_FRAMES_CONFIG.
BASE_OTHER_PARAMS = {
    "fromUser": "false",
    "include_events": "false", # Generally, we don't need event overlays for metric values
    "no_graph": "true"         # We want the summary value/toplist, not full graph pointlists
}

BASE_API_URL = f"https://p.us5.datadoghq.com/dashboard/shared_widget_update/{DASHBOARD_ID}"
REQUEST_INTERVAL_SECONDS = 0.5 
OUTPUT_FILE = "datadog_helius_usage.json"
LOG_FILE = "datadog_fetcher.log"

TIME_FRAMES_CONFIG = {
    "last_15_minutes": {"td_unit": "minute", "td_value": "15"},
    "last_hour": {"td_unit": "minute", "td_value": "60"},
    "last_24_hours": {"td_unit": "hour", "td_value": "24"},
    "last_7_days": {"td_unit": "day", "td_value": "7"},
    "last_3_months": {"td_unit": "month", "td_value": "3"}
}
TIME_FRAMES_ORDER = ["last_15_minutes", "last_hour", "last_24_hours", "last_7_days", "last_3_months"]

# --- Logging ---
def log_message(message: str, is_error: bool = False):
    prefix = "[ERROR]" if is_error else "[INFO]"
    log_entry = f"{datetime.now(timezone.utc).isoformat()} {prefix} {message}"
    print(log_entry)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")

# --- Helper Functions ---
def parse_numeric_value(val_str: Optional[str]) -> int:
    if val_str is None: return 0
    val_str = str(val_str).strip().lower()
    if not val_str or val_str == "-" or "no data" in val_str or "loading" in val_str:
        return 0
    
    multiplier = 1
    num_part = val_str
    if val_str.endswith('k'):
        num_part = val_str[:-1]
        multiplier = 1000
    elif val_str.endswith('m'):
        num_part = val_str[:-1]
        multiplier = 1_000_000
    elif val_str.endswith('b'):
        num_part = val_str[:-1]
        multiplier = 1_000_000_000
    try:
        return int(float(num_part) * multiplier)
    except ValueError:
        log_message(f"Could not parse numeric value: '{val_str}'", is_error=True)
        return 0

def parse_widget_json(widget_name: str, widget_id: str, response_json: Dict[str, Any]) -> Any:
    widget_data_list = response_json.get("responses", {}).get(widget_id)
    if not widget_data_list or not isinstance(widget_data_list, list) or not widget_data_list[0]:
        log_message(f"Data for widget_id '{widget_id}' (name: {widget_name}) not found in response_json['responses']. Raw JSON: {json.dumps(response_json, indent=2)[:500]}", is_error=True)
        return None

    actual_widget_response = widget_data_list[0]
    widget_type = actual_widget_response.get("type")

    if widget_name == "successful_requests":
        val_str = None
        if actual_widget_response.get("attributes") and actual_widget_response["attributes"].get("last_value") is not None:
             val_str = str(actual_widget_response["attributes"]["last_value"])
        elif "value" in actual_widget_response:
            val_str = str(actual_widget_response["value"])
        elif (actual_widget_response.get("data") and isinstance(actual_widget_response.get("data"), list) and 
              actual_widget_response["data"] and isinstance(actual_widget_response["data"][0], dict) and
              actual_widget_response["data"][0].get("attributes", {}).get("value") is not None):
            val_str = str(actual_widget_response["data"][0]["attributes"]["value"])
        
        if val_str is not None:
            return parse_numeric_value(val_str)
        
        log_message(f"Could not extract value for 'successful_requests' (widget_id: {widget_id}). Widget JSON: {json.dumps(actual_widget_response, indent=2)[:500]}", is_error=True)
        return 0

    elif widget_name == "method_breakdown":
        if widget_type != "toplist_response":
            log_message(f"Expected 'toplist_response' for 'method_breakdown' (widget_id: {widget_id}) but got '{widget_type}'. Widget JSON: {json.dumps(actual_widget_response, indent=2)[:500]}", is_error=True)
            return {}
        try:
            data_attributes = actual_widget_response.get("data", [{}])[0].get("attributes", {})
            inputs_list = data_attributes.get("inputs", [])
            methods_data = {}
            if inputs_list:
                for item in inputs_list:
                    method_name_full = item.get("defaultLabel")
                    if not method_name_full and item.get("metadata", {}).get("groupTags"):
                        method_name_full = item["metadata"]["groupTags"][0]
                    method_name = method_name_full.split(":")[-1] if method_name_full and ":" in method_name_full else method_name_full
                    value = item.get("value")
                    if method_name and value is not None:
                        methods_data[method_name] = int(value)
                return methods_data
            else:
                log_message(f"'inputs' list not found/empty for 'method_breakdown' (widget_id: {widget_id}). Widget JSON: {json.dumps(actual_widget_response, indent=2)[:500]}", is_error=True)
                return {}
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            log_message(f"Error parsing 'method_breakdown' (widget_id: {widget_id}): {e}. Widget JSON: {json.dumps(actual_widget_response, indent=2)[:500]}", is_error=True)
            return {}
            
    log_message(f"No specific parser for widget_name '{widget_name}' (widget_id: {widget_id}). Widget JSON: {json.dumps(actual_widget_response, indent=2)[:500]}", is_error=True)
    return None

def fetch_widget_data(widget_name: str, widget_id: str, time_frame_config: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if not widget_id or "YOUR_" in widget_id.upper():
        log_message(f"Skipping {widget_name} (ID: {widget_id}) due to placeholder WIDGET_ID.", is_error=True)
        return None

    params = {
        **STATIC_TPL_VARS,
        **BASE_OTHER_PARAMS, # Static params like fromUser, no_graph, include_events
        "td_type": "live",   # Crucial for making td_unit and td_value work as "last X"
        "td_unit": time_frame_config["td_unit"],
        "td_value": time_frame_config["td_value"],
    }
    
    endpoint_url = f"{BASE_API_URL}/{widget_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://p.us5.datadoghq.com/sb/{DASHBOARD_ID}" # Good practice
    }

    try:
        # For more verbose debugging of the exact request:
        # full_url_for_log = f"{endpoint_url}?{urlencode(params)}"
        # log_message(f"Fetching for {widget_name} ({widget_id}) | Timeframe: {time_frame_config} | URL: {full_url_for_log}")
        
        response = requests.get(endpoint_url, params=params, headers=headers, timeout=30)
        
        # Optional: log rate limit info for every call if needed for intense debugging
        # log_message(f"  RateLimit-Remaining for {widget_name} ({time_frame_config['td_value']}{time_frame_config['td_unit'][0]}): {response.headers.get('x-ratelimit-remaining')}")
        
        response.raise_for_status()
        response_json = response.json()
        return parse_widget_json(widget_name, widget_id, response_json)
        
    except requests.exceptions.HTTPError as http_err:
        log_message(f"HTTP error for {widget_name} ({widget_id}, {time_frame_config}): {http_err}. Response: {http_err.response.text[:200] if http_err.response else 'No response body'}", is_error=True)
    except requests.exceptions.RequestException as req_err:
        log_message(f"Request error for {widget_name} ({widget_id}, {time_frame_config}): {req_err}", is_error=True)
    except json.JSONDecodeError as json_err:
        # It's useful to see the response text when JSON decoding fails
        response_text_snippet = response.text[:200] if 'response' in locals() and hasattr(response, 'text') else "Response object not available"
        log_message(f"JSON decode error for {widget_name} ({widget_id}, {time_frame_config}): {json_err}. Response: {response_text_snippet}", is_error=True)
    except Exception as err:
        log_message(f"Unexpected error for {widget_name} ({widget_id}, {time_frame_config}): {err}", is_error=True)
        import traceback
        traceback.print_exc()
    return None

def main():
    log_message("Starting Datadog metrics fetcher.")
    
    if "YOUR_SUCCESSFUL_REQUESTS_WIDGET_ID" in WIDGET_IDS["successful_requests"].upper():
        log_message("WARNING: 'successful_requests' WIDGET_ID is a placeholder. It will be skipped.", is_error=True)

    overall_results = {
        "last_fetched_utc": datetime.now(timezone.utc).isoformat(),
        "timeframes": {}
    }

    for timeframe_name in TIME_FRAMES_ORDER:
        time_params_from_config = TIME_FRAMES_CONFIG[timeframe_name]
        log_message(f"Fetching data for timeframe: {timeframe_name} ({time_params_from_config['td_value']}{time_params_from_config['td_unit'][0]})")
        
        current_timeframe_metrics = {
            "balances": 0,
            "transactionhistory": 0,
            "successful_requests_total_for_period": 0,
        }

        sr_widget_id = WIDGET_IDS["successful_requests"]
        if not "YOUR_" in sr_widget_id.upper():
            sr_data = fetch_widget_data("successful_requests", sr_widget_id, time_params_from_config)
            if isinstance(sr_data, int):
                current_timeframe_metrics["successful_requests_total_for_period"] = sr_data
            time.sleep(REQUEST_INTERVAL_SECONDS)

        mb_widget_id = WIDGET_IDS["method_breakdown"]
        if not "YOUR_" in mb_widget_id.upper():
            mb_data = fetch_widget_data("method_breakdown", mb_widget_id, time_params_from_config)
            if isinstance(mb_data, dict):
                current_timeframe_metrics["balances"] = mb_data.get("balances", 0)
                current_timeframe_metrics["transactionhistory"] = mb_data.get("transactionhistory", 0)
                for method, count in mb_data.items():
                     if method not in current_timeframe_metrics:
                        current_timeframe_metrics[method] = count
            time.sleep(REQUEST_INTERVAL_SECONDS)
        
        overall_results["timeframes"][timeframe_name] = {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "params_used": time_params_from_config, # Store the actual td_unit/td_value used
            "usage_data": current_timeframe_metrics
        }
        log_message(f"Completed fetching for {timeframe_name}. Usage: {current_timeframe_metrics}")

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(overall_results, f, indent=2)
        log_message(f"All data saved to {OUTPUT_FILE}")
    except IOError as e:
        log_message(f"Could not write data to {OUTPUT_FILE}: {e}", is_error=True)

    log_message("Datadog metrics fetcher finished.")

if __name__ == "__main__":
    if WIDGET_IDS["successful_requests"].upper() == "YOUR_SUCCESSFUL_REQUESTS_WIDGET_ID":
        print("\n" + "="*50)
        print("!! PLEASE UPDATE 'successful_requests' WIDGET_ID IN THE SCRIPT OR .env !!")
        print("You can find this using your browser's Developer Tools (Network tab)")
        print("while loading the Datadog dashboard. Filter for 'shared_widget_update'.")
        print("="*50 + "\n")
    main()