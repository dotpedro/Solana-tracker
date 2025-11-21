

# eco_mode_config.py
import json
import os
import traceback

# Define the path relative to this script's location
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "eco_mode_config.json")

def _load_config():
    """Internal function to safely load the JSON configuration."""
    if not os.path.exists(CONFIG_PATH):
        print(f"[!] Eco Mode Error: Config file not found at {CONFIG_PATH}")
        # Return a default structure to prevent crashes, assuming eco mode off
        return {
            "eco_mode": False,
            "parameters": {
                "eco_on": {},
                "eco_off": {}
            }
        }
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"[!] Eco Mode Error: Invalid JSON in {CONFIG_PATH}")
        traceback.print_exc()
        # Return default structure on decode error
        return {
            "eco_mode": False,
            "parameters": {
                "eco_on": {},
                "eco_off": {}
            }
        }
    except Exception as e:
        print(f"[!] Eco Mode Error: Failed to load config: {e}")
        traceback.print_exc()
        # Return default structure on other errors
        return {
            "eco_mode": False,
            "parameters": {
                "eco_on": {},
                "eco_off": {}
            }
        }

def _save_config(config_data):
    """Internal function to safely save the JSON configuration."""
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2)
        return True
    except IOError as e:
        print(f"[!] Eco Mode Error: Failed to save config to {CONFIG_PATH}: {e}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"[!] Eco Mode Error: Unexpected error saving config: {e}")
        traceback.print_exc()
        return False

def load_current_params():
    """
    Loads the parameters corresponding to the current eco mode setting.
    Returns the dictionary of parameters for the active mode.
    If config fails to load or a mode is missing, returns an empty dict
    and prints an error.
    """
    config = _load_config()
    mode_key = "eco_on" if config.get("eco_mode") else "eco_off"
    
    params = config.get("parameters", {}).get(mode_key)
    
    if params is None:
        print(f"[!] Eco Mode Error: Parameters for mode '{mode_key}' not found in config!")
        # Fallback to the *other* mode if available, otherwise empty
        fallback_mode_key = "eco_off" if mode_key == "eco_on" else "eco_on"
        params = config.get("parameters", {}).get(fallback_mode_key, {})
        if params:
             print(f"[!] Eco Mode Warning: Falling back to '{fallback_mode_key}' parameters.")
        else:
             print(f"[!] Eco Mode Error: No parameter sets found. Returning empty params.")
             params = {}
             
    return params

def set_eco_mode(enable: bool):
    """Sets the eco_mode flag in the configuration file."""
    config = _load_config()
    if "eco_mode" not in config:
        print("[!] Eco Mode Warning: 'eco_mode' key missing from config. Adding it.")
    
    config["eco_mode"] = enable
    success = _save_config(config)
    if success:
        mode_text = "ENABLED" if enable else "DISABLED"
        print(f"[✓] Eco Mode: Set to {mode_text} in {CONFIG_PATH}")
    return success


def is_eco_mode():
    """Checks if eco mode is currently enabled in the configuration file."""
    config = _load_config()
    return config.get("eco_mode", False)

# Example usage (optional, for testing the module directly)
if __name__ == "__main__":
    print(f"Checking configuration file at: {CONFIG_PATH}")

    current_mode_enabled = is_eco_mode()
    print(f"Is Eco Mode currently enabled? {current_mode_enabled}")

    print("\nLoading current parameters...")
    params = load_current_params()
    if params:
        print(f"Loaded {len(params)} parameters for {'eco_on' if current_mode_enabled else 'eco_off'} mode.")
        print(f"  Example - ANALYSIS_INTERVAL_SECONDS: {params.get('ANALYSIS_INTERVAL_SECONDS', 'Not Found')}")
    else:
        print("Failed to load parameters.")

    print("\nTesting toggle (this will modify the file):")
    # Test setting ON
    print("Setting Eco Mode ON...")
    set_eco_mode(True)
    print(f"Is Eco Mode enabled after set(True)? {is_eco_mode()}")
    params_on = load_current_params()
    print(f"  Loaded ANALYSIS_INTERVAL_SECONDS: {params_on.get('ANALYSIS_INTERVAL_SECONDS', 'Not Found')}")


    # Test setting OFF
    print("\nSetting Eco Mode OFF...")
    set_eco_mode(False)
    print(f"Is Eco Mode enabled after set(False)? {is_eco_mode()}")
    params_off = load_current_params()
    print(f"  Loaded ANALYSIS_INTERVAL_SECONDS: {params_off.get('ANALYSIS_INTERVAL_SECONDS', 'Not Found')}")
