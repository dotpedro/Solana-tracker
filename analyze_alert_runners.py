#!/usr/bin/env python3
"""
Post-Alert Runner Analyzer for Solana Tokens.
Reads alert_history.json, labels tokens based on post-alert performance,
trains a model, and predicts runner likelihood for new/updated alerts.
Version: 1.0
"""

import os
import json
import time
import datetime
import traceback
import re
from typing import Dict, Any, Optional, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report

from dotenv import load_dotenv

# --- User Modules (ensure these are in PYTHONPATH or same directory) ---
try:
    import birdeye_api
    import helius_analyzer # For token creation time primarily
    import cache_manager # Used by birdeye_api and helius_analyzer
    print("[✓] analyze_alert_runners: Imported birdeye_api, helius_analyzer, cache_manager.")
    # Initialize cache DB if needed by modules (birdeye_api does this)
    # cache_manager.init_db() # birdeye_api should call this
except ImportError as e:
    print(f"[!] CRITICAL IMPORT ERROR: {e}. analyze_alert_runners will not function.")
    print("    Ensure birdeye_api.py, helius_analyzer.py, and cache_manager.py are accessible.")
    exit(1)

load_dotenv()

# --- Configuration ---
REPORTS_ROOT = os.path.join("reports", "holdings")
HISTORY_FILE = os.path.join(REPORTS_ROOT, "alert_history.json")
TRAINING_DATA_FILE = os.path.join(REPORTS_ROOT, "runner_training_data.json")
MODEL_FILE = os.path.join(REPORTS_ROOT, "runner_analysis_model.joblib")
LOG_FILE = os.path.join(REPORTS_ROOT, "analyze_alert_runners.log")

# --- Analysis Parameters ---
POST_ALERT_WAIT_PERIOD_SECONDS = 30 * 60  # 30 minutes to wait before labeling
LOOP_SLEEP_SECONDS = 5 * 60             # Run analysis loop every 5 minutes
MIN_SAMPLES_FOR_TRAINING = 20           # Minimum labeled samples to train/retrain model
PRICE_INCREASE_THRESHOLD_RUNNER = 0.50  # 50% price increase for "runner" label
LIQUIDITY_INCREASE_THRESHOLD_RUNNER = 0.20 # 20% liquidity increase
HOLDERS_INCREASE_ABS_THRESHOLD_RUNNER = 5 # Absolute increase in holders (Birdeye total)
MIN_ALERT_TIME_PRICE_FOR_LABELING = 0.00000001 # Avoid division by zero for tiny prices

# --- Caching for fetched aux features (in-memory for one run) ---
_AUX_FEATURE_CACHE: Dict[str, Dict[str, Any]] = {} # Stores fetched creation_ts, security_flags per mint

# --- Logging ---
def log_message(message: str, is_error: bool = False):
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    if is_error:
        print(traceback.format_exc(), file=open(LOG_FILE, "a", encoding="utf-8")) # Log traceback for errors
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")

# --- Data Utilities ---
def load_json_data(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log_message(f"File not found: {path}. Returning default.")
        return default if default is not None else {}
    except json.JSONDecodeError:
        log_message(f"Error decoding JSON from {path}. Corrupted? Returning default.", is_error=True)
        # Optionally, try to recover or backup the corrupted file here
        return default if default is not None else {}
    except Exception as e:
        log_message(f"Failed to load {path}: {e}", is_error=True)
        return default if default is not None else {}

def save_json_data(path: str, data: Any):
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        log_message(f"Saved data to {path}")
    except Exception as e:
        log_message(f"Failed to save data to {path}: {e}", is_error=True)

# --- Feature Extraction & Labeling ---

def get_token_creation_info_cached(mint_address: str) -> Optional[int]:
    """Fetches and caches token creation timestamp (Unix)."""
    if mint_address in _AUX_FEATURE_CACHE and "creation_timestamp_unix" in _AUX_FEATURE_CACHE[mint_address]:
        return _AUX_FEATURE_CACHE[mint_address]["creation_timestamp_unix"]

    creation_info_bds = birdeye_api.token_creation_info(mint_address)
    if creation_info_bds and creation_info_bds.get("creationTime"): # Birdeye gives Unix timestamp
        ts = int(creation_info_bds["creationTime"])
        if mint_address not in _AUX_FEATURE_CACHE: _AUX_FEATURE_CACHE[mint_address] = {}
        _AUX_FEATURE_CACHE[mint_address]["creation_timestamp_unix"] = ts
        return ts

    # Fallback to Helius (more complex to parse, Birdeye preferred for this)
    # log_message(f"Birdeye creation info failed for {mint_address}. Helius fallback not implemented for creation time in this script yet.")
    # For simplicity, if Birdeye fails, we might return None or try Helius if essential
    # helius_txs = helius_analyzer.get_earliest_parsed_transactions_v0(mint_address, target_count=5, page_limit=1)
    # if helius_txs and helius_txs[0].get('timestamp'):
    #     ts = helius_txs[0]['timestamp'] # This is oldest tx involving mint, not necessarily creation
    #     # Be careful, this might not be the true creation time.
    #     if mint_address not in _AUX_FEATURE_CACHE: _AUX_FEATURE_CACHE[mint_address] = {}
    #     _AUX_FEATURE_CACHE[mint_address]["creation_timestamp_unix"] = ts
    #     return ts
    return None

def get_token_security_info_cached(mint_address: str) -> Dict[str, Any]:
    """Fetches and caches token security flags from Birdeye."""
    if mint_address in _AUX_FEATURE_CACHE and "security_flags" in _AUX_FEATURE_CACHE[mint_address]:
        return _AUX_FEATURE_CACHE[mint_address]["security_flags"]

    sec_data = birdeye_api.token_security(mint_address)
    flags = {
        "is_mutable": None,
        "can_freeze": None,
        "has_freeze_authority": None, # More specific
        "has_mint_authority": None, # Birdeye sec doesn't typically show this well for SPL
        # Other flags like creator/update authority checks can be added if birdeye_api returns them
    }
    if sec_data:
        flags["is_mutable"] = bool(sec_data.get("mutableMetadata"))
        freeze_auth = sec_data.get("freezeAuthority")
        flags["has_freeze_authority"] = bool(freeze_auth and freeze_auth not in ["11111111111111111111111111111111", None])
        flags["can_freeze"] = bool(sec_data.get("freezeable")) # Broader flag

    if mint_address not in _AUX_FEATURE_CACHE: _AUX_FEATURE_CACHE[mint_address] = {}
    _AUX_FEATURE_CACHE[mint_address]["security_flags"] = flags
    return flags

def parse_alert_reason(reason_str: Optional[str]) -> Dict[str, Any]:
    parsed = {
        "reason_is_new": 0, "reason_is_mc_pump": 0, "reason_mc_pump_pct": 0.0,
        "reason_is_vol_pump": 0, "reason_vol_pump_pct": 0.0,
        "reason_is_holder_increase": 0, "reason_holder_increase_abs": 0
    }
    if not reason_str: return parsed

    if "New Validated Listing" in reason_str: parsed["reason_is_new"] = 1
    
    mc_match = re.search(r"MC ↑([\d\.]+)%", reason_str)
    if mc_match:
        parsed["reason_is_mc_pump"] = 1
        try: parsed["reason_mc_pump_pct"] = float(mc_match.group(1)) / 100.0
        except ValueError: pass

    vol_match = re.search(r"Vol ↑([\d\.]+)%", reason_str)
    if vol_match:
        parsed["reason_is_vol_pump"] = 1
        try: parsed["reason_vol_pump_pct"] = float(vol_match.group(1)) / 100.0
        except ValueError: pass
    
    holder_match = re.search(r"Mon\.Holders \+([\d]+)", reason_str)
    if holder_match:
        parsed["reason_is_holder_increase"] = 1
        try: parsed["reason_holder_increase_abs"] = int(holder_match.group(1))
        except ValueError: pass
    return parsed


def extract_alert_time_features(mint_address: str, history_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extracts features from alert_history.json entry and fetches auxiliary ones."""
    try:
        features = {}
        
        # Direct from history_entry (at alert time)
        features["price_at_alert"] = float(history_entry.get("price", 0))
        features["market_cap_at_alert"] = float(history_entry.get("market_cap", 0))
        features["fdv_at_alert"] = float(history_entry.get("fdv", 0))
        features["volume24h_at_alert"] = float(history_entry.get("volume24h", 0))
        features["monitored_holders_at_alert"] = int(history_entry.get("total_holders_in_set", 0))
        
        features["symbol_length"] = len(history_entry.get("symbol", ""))
        features["name_length"] = len(history_entry.get("name", ""))

        last_alert_ts = int(history_entry["last_alert_timestamp"])
        first_alert_ts = int(history_entry.get("first_alert_timestamp", last_alert_ts))
        features["time_on_radar_seconds"] = last_alert_ts - first_alert_ts

        # Parse alert reason
        parsed_reason = parse_alert_reason(history_entry.get("last_alert_reason"))
        features.update(parsed_reason)

        # Fetch/Cache auxiliary features (these are static for the token, but relative to alert time for age)
        creation_ts_unix = get_token_creation_info_cached(mint_address)
        if creation_ts_unix:
            features["creation_timestamp_unix"] = creation_ts_unix
            features["age_at_alert_seconds"] = last_alert_ts - creation_ts_unix
        else: # Handle missing creation time
            features["creation_timestamp_unix"] = None # Will be imputed
            features["age_at_alert_seconds"] = None # Will be imputed
        
        security_flags = get_token_security_info_cached(mint_address)
        features["security_is_mutable"] = 1 if security_flags.get("is_mutable") else 0 if security_flags.get("is_mutable") is False else None
        features["security_can_freeze"] = 1 if security_flags.get("can_freeze") else 0 if security_flags.get("can_freeze") is False else None
        features["security_has_freeze_authority"] = 1 if security_flags.get("has_freeze_authority") else 0 if security_flags.get("has_freeze_authority") is False else None
        
        # Add mint for reference, not as a direct feature for typical models
        # features["mint_address"] = mint_address 
        return features

    except Exception as e:
        log_message(f"Error extracting features for {mint_address}: {e}", is_error=True)
        return None

def calculate_performance_label(
    mint_address: str,
    alert_time_data: Dict[str, Any] # The history_entry for the token
) -> Optional[int]:
    """
    Fetches current data and compares to alert-time data to assign a label.
    Returns 1 if runner, 0 if not, None if data is insufficient.
    """
    alert_price = alert_time_data.get("price")
    # Use FDV as a proxy for alert-time liquidity if liquidity not stored. Risky but a fallback.
    alert_liquidity_proxy = alert_time_data.get("fdv") # or market_cap if fdv is also missing
    alert_volume = alert_time_data.get("volume24h")
    # BD S total holders not in history, use monitored as a weak proxy or fetch historical if possible (hard)

    if alert_price is None or alert_price < MIN_ALERT_TIME_PRICE_FOR_LABELING : # or alert_liquidity_proxy is None:
        log_message(f"Labeling: Insufficient alert-time data for {mint_address} (price: {alert_price}). Skipping label.", is_error=True)
        return None

    try:
        current_market = birdeye_api.market_data(mint_address)
        current_trade = birdeye_api.trade_data(mint_address)
        time.sleep(0.5) # Small delay after API calls

        if not current_market or not current_trade:
            log_message(f"Labeling: Failed to fetch current Birdeye data for {mint_address}. Skipping label.", is_error=True)
            return None

        current_price = current_market.get("price")
        current_liquidity = current_market.get("liquidity")
        current_total_holders = (current_trade.get("holder") or {}).get("unique") # BDS v3 has holder.unique or holder.total sometimes
        if current_total_holders is None: # Fallback parsing for BDS trade_data
             current_total_holders = current_trade.get("holderCount") or (current_trade.get("holder",{}) or {}).get("count")


        if current_price is None: # current_liquidity is None or current_total_holders is None: # Liquidity can be 0 for new tokens
            log_message(f"Labeling: Missing critical current data fields for {mint_address} (price: {current_price}, liq: {current_liquidity}, holders: {current_total_holders}). Skipping label.", is_error=True)
            return None
        
        # Calculate changes
        price_change_pct = (current_price - alert_price) / alert_price if alert_price > 0 else float('inf')
        
        ran_on_price = price_change_pct >= PRICE_INCREASE_THRESHOLD_RUNNER
        
        # Liquidity check (optional, can be tricky for new tokens)
        ran_on_liquidity = False
        if alert_liquidity_proxy is not None and current_liquidity is not None and alert_liquidity_proxy > 0:
            liq_change_pct = (current_liquidity - alert_liquidity_proxy) / alert_liquidity_proxy
            ran_on_liquidity = liq_change_pct >= LIQUIDITY_INCREASE_THRESHOLD_RUNNER
        elif current_liquidity is not None and current_liquidity > (alert_liquidity_proxy or 0): # If it just gained any liquidity
             ran_on_liquidity = True


        # Holder check (optional) - Birdeye total holders vs what? History has monitored.
        # For now, let's say if price is main driver.
        # If we had alert_time_total_holders, we could compare.
        # ran_on_holders = (current_total_holders - (alert_time_data.get("total_holders_bds_at_alert", 0))) >= HOLDERS_INCREASE_ABS_THRESHOLD_RUNNER

        log_message(f"Labeling {mint_address}: Price Δ%{price_change_pct*100:.1f}. Liq now: {current_liquidity}, Alert Liq Proxy: {alert_liquidity_proxy}. Holders now: {current_total_holders}")

        if ran_on_price or ran_on_liquidity: # Main condition is price, liq is bonus
            log_message(f"  -> Labeled as RUNNER (1) for {mint_address}")
            return 1
        else:
            log_message(f"  -> Labeled as NOT RUNNER (0) for {mint_address}")
            return 0

    except Exception as e:
        log_message(f"Error during labeling for {mint_address}: {e}", is_error=True)
        return None

# --- Model Training & Prediction ---
MODEL_PIPELINE = None
FEATURE_COLUMNS_ORDER = [] # To ensure consistency

def get_feature_columns():
    # Define the order of features for the model
    # This should match the columns of the DataFrame used for training
    # Must be updated if extract_alert_time_features changes its output keys
    # Note: 'mint_address' or other identifiers should not be in here.
    return [
        'price_at_alert', 'market_cap_at_alert', 'fdv_at_alert', 'volume24h_at_alert',
        'monitored_holders_at_alert', 'symbol_length', 'name_length',
        'time_on_radar_seconds', 'reason_is_new', 'reason_is_mc_pump',
        'reason_mc_pump_pct', 'reason_is_vol_pump', 'reason_vol_pump_pct',
        'reason_is_holder_increase', 'reason_holder_increase_abs',
        'creation_timestamp_unix', 'age_at_alert_seconds',
        'security_is_mutable', 'security_can_freeze', 'security_has_freeze_authority'
    ]

def build_model_pipeline():
    global FEATURE_COLUMNS_ORDER
    FEATURE_COLUMNS_ORDER = get_feature_columns()

    numeric_features = [
        'price_at_alert', 'market_cap_at_alert', 'fdv_at_alert', 'volume24h_at_alert',
        'monitored_holders_at_alert', 'symbol_length', 'name_length',
        'time_on_radar_seconds', 'reason_mc_pump_pct', 'reason_vol_pump_pct',
        'reason_holder_increase_abs', 'creation_timestamp_unix', 'age_at_alert_seconds'
    ]
    # Binary features are already 0/1, but SimpleImputer can handle NaNs if they are numeric
    # We'll treat them as numeric and impute, then scale.
    # If some are strictly categorical (even if 0/1), they might go into a one-hot encoder pipe.
    # For simplicity, let's assume these binary flags from parsing/security are fine with numeric pipe.
    binary_like_features = [
        'reason_is_new', 'reason_is_mc_pump', 'reason_is_vol_pump', 'reason_is_holder_increase',
        'security_is_mutable', 'security_can_freeze', 'security_has_freeze_authority'
    ]
    
    # Combine all features that will go through numeric processing
    all_numeric_features = numeric_features + binary_like_features

    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')), # Handles NaN
        ('scaler', StandardScaler())
    ])

    # No categorical features that need one-hot encoding in this simple setup yet
    # If 'last_alert_reason' was a raw string category, it would go here.
    # preprocessor = ColumnTransformer(
    #     transformers=[
    #         ('num', numeric_transformer, all_numeric_features)
    #     ], 
    #     remainder='drop' # Drop any columns not specified
    # )
    # Since all features are processed by numeric_transformer (after parsing):
    preprocessor = numeric_transformer


    # The pipeline will apply to a DataFrame with columns in FEATURE_COLUMNS_ORDER
    # So the preprocessor needs to know which columns to apply to.
    # A bit tricky if we pass the whole DataFrame.
    # Easiest if the DataFrame passed to pipeline.fit only contains these features.

    pipeline = Pipeline(steps=[
        # ('preprocessor', preprocessor), # If using ColumnTransformer
        # If just one transformer for all selected columns:
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'))
    ])
    return pipeline

def train_model(training_data_path: str) -> Optional[Pipeline]:
    global MODEL_PIPELINE, FEATURE_COLUMNS_ORDER
    
    raw_training_data = load_json_data(training_data_path, [])
    if len(raw_training_data) < MIN_SAMPLES_FOR_TRAINING:
        log_message(f"Not enough samples ({len(raw_training_data)}) to train model. Need {MIN_SAMPLES_FOR_TRAINING}.")
        return None

    # Convert to DataFrame: each item in raw_training_data is { "features": {...}, "label": N, "mint": ... }
    features_list = [item['features'] for item in raw_training_data]
    labels_list = [item['label'] for item in raw_training_data]

    df = pd.DataFrame(features_list)
    
    # Ensure FEATURE_COLUMNS_ORDER is set
    if not FEATURE_COLUMNS_ORDER: FEATURE_COLUMNS_ORDER = get_feature_columns()
    
    # Reorder/select columns to match FEATURE_COLUMNS_ORDER and handle missing ones
    df_processed = pd.DataFrame()
    for col in FEATURE_COLUMNS_ORDER:
        if col in df:
            df_processed[col] = df[col]
        else:
            df_processed[col] = np.nan # Add as NaN, imputer will handle

    X = df_processed
    y = pd.Series(labels_list)

    if X.empty:
        log_message("No features to train on after processing. Aborting model training.", is_error=True)
        return None

    # Split data for evaluation (optional, but good practice)
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y if len(y.unique()) > 1 else None)
    # For now, train on all available data:
    X_train, y_train = X, y


    log_message(f"Training model with {len(X_train)} samples...")
    MODEL_PIPELINE = build_model_pipeline()
    try:
        MODEL_PIPELINE.fit(X_train, y_train)
        joblib.dump(MODEL_PIPELINE, MODEL_FILE)
        log_message(f"Model trained and saved to {MODEL_FILE}")

        # Log classification report on training data (not ideal, but gives some insight)
        # y_pred_train = MODEL_PIPELINE.predict(X_train)
        # report = classification_report(y_train, y_pred_train, zero_division=0)
        # log_message(f"Training Data Classification Report:\n{report}")
        
        return MODEL_PIPELINE
    except Exception as e:
        log_message(f"Error training model: {e}", is_error=True)
        MODEL_PIPELINE = None # Invalidate
        return None


def predict_runner(features_dict: Dict[str, Any]) -> Optional[Tuple[float, int]]:
    global MODEL_PIPELINE, FEATURE_COLUMNS_ORDER
    if MODEL_PIPELINE is None:
        try:
            MODEL_PIPELINE = joblib.load(MODEL_FILE)
            log_message("Loaded existing model for prediction.")
            if not FEATURE_COLUMNS_ORDER: FEATURE_COLUMNS_ORDER = get_feature_columns() # Ensure it's populated
        except FileNotFoundError:
            log_message("Model file not found. Cannot predict.", is_error=True)
            return None
        except Exception as e:
            log_message(f"Error loading model: {e}", is_error=True)
            return None
    
    if not FEATURE_COLUMNS_ORDER:
        log_message("FEATURE_COLUMNS_ORDER not set. Cannot predict safely.", is_error=True)
        return None

    # Create a DataFrame with a single row for prediction, ensuring correct column order and presence
    df_predict = pd.DataFrame([features_dict])
    df_processed_predict = pd.DataFrame()
    for col in FEATURE_COLUMNS_ORDER:
        if col in df_predict:
            df_processed_predict[col] = df_predict[col]
        else:
            df_processed_predict[col] = np.nan # Model's imputer should handle this

    try:
        pred_proba = MODEL_PIPELINE.predict_proba(df_processed_predict) # [[prob_class_0, prob_class_1]]
        runner_score = float(pred_proba[0][1]) # Probability of being a runner (class 1)
        
        # Get actual prediction based on threshold (usually 0.5)
        # prediction_label = int(MODEL_PIPELINE.predict(df_processed_predict)[0])
        # Use the score to determine label if needed, or let classifier decide
        prediction_label = 1 if runner_score >= 0.5 else 0 

        return runner_score, prediction_label
    except Exception as e:
        log_message(f"Error during prediction: {e}", is_error=True)
        return None

# --- Main Loop ---
# --- Main Loop ---
def main_analysis_loop():
    global MODEL_PIPELINE
    log_message("--- Starting Post-Alert Runner Analyzer ---")
    
    if os.path.exists(MODEL_FILE):
        try:
            MODEL_PIPELINE = joblib.load(MODEL_FILE)
            log_message(f"Successfully loaded pre-existing model from {MODEL_FILE}")
        except Exception as e:
            log_message(f"Could not load pre-existing model from {MODEL_FILE}: {e}", is_error=True)
            MODEL_PIPELINE = None # Ensure it's None if loading failed
    else:
        log_message(f"No pre-existing model found at {MODEL_FILE}. Will train if data allows.")
        MODEL_PIPELINE = None

    while True:
        try:
            current_time_unix = int(time.time())
            alert_history = load_json_data(HISTORY_FILE, {}) # This is still the full history dict
            if not alert_history:
                log_message("Alert history is empty or failed to load. Waiting...")
                time.sleep(LOOP_SLEEP_SECONDS)
                continue

            # --- SORT ALERT ITEMS BY LATEST TIMESTAMP FIRST ---
            # This creates a list of (mint, entry_data) tuples, sorted
            sorted_alert_items = sorted(
                alert_history.items(),
                key=lambda item: item[1].get("last_alert_timestamp", 0), # item[1] is the entry_data dict
                reverse=True  # Newest first
            )
            # ----------------------------------------------------

            training_data_updated_this_run = False
            history_modified_this_run = False # This flag tracks if the alert_history dict itself was modified
            new_training_samples = [] 

            # --- Pass 1: Labeling ---
            # Iterate over the sorted list of items
            log_message(f"Processing {len(sorted_alert_items)} tokens for labeling (newest first)...")
            for mint, entry_data in sorted_alert_items: # Use the sorted list here
                if not isinstance(entry_data, dict) or "last_alert_timestamp" not in entry_data:
                    log_message(f"Skipping invalid entry for mint {mint}", is_error=True)
                    continue

                last_alert_ts = int(entry_data["last_alert_timestamp"])
                
                time_since_alert = current_time_unix - last_alert_ts
                eligible_for_labeling = time_since_alert > POST_ALERT_WAIT_PERIOD_SECONDS
                needs_new_label = entry_data.get("runner_label_for_alert_at") != last_alert_ts

                if eligible_for_labeling and needs_new_label:
                    log_message(f"Token {mint[:6]}... eligible for labeling (alerted {time_since_alert // 60} mins ago).")
                    alert_time_features = extract_alert_time_features(mint, entry_data)
                    if alert_time_features:
                        label = calculate_performance_label(mint, entry_data)
                        if label is not None:
                            new_training_samples.append({
                                "mint": mint,
                                "features": alert_time_features,
                                "label": label,
                                "labeled_at_unix": current_time_unix,
                                "label_for_alert_at": last_alert_ts
                            })
                            # Update the original alert_history dictionary
                            alert_history[mint]["runner_label"] = label
                            alert_history[mint]["runner_label_timestamp"] = current_time_unix
                            alert_history[mint]["runner_label_for_alert_at"] = last_alert_ts
                            history_modified_this_run = True
                            training_data_updated_this_run = True 
                    else:
                        log_message(f"Could not extract features for {mint} during labeling.")
            
            if new_training_samples:
                current_training_data = load_json_data(TRAINING_DATA_FILE, [])
                current_training_data.extend(new_training_samples)
                save_json_data(TRAINING_DATA_FILE, current_training_data)
                log_message(f"Appended {len(new_training_samples)} new samples to training data.")

            if training_data_updated_this_run or MODEL_PIPELINE is None: 
                log_message("Attempting to train/retrain model...")
                trained_model = train_model(TRAINING_DATA_FILE)
                if trained_model:
                    MODEL_PIPELINE = trained_model 
                    log_message("Model training/update successful.")
                else:
                    log_message("Model training/update failed or deferred.")
            
            # --- Pass 3: Prediction ---
            if MODEL_PIPELINE: 
                log_message(f"Processing {len(sorted_alert_items)} tokens for prediction (newest first)...")
                # Iterate over the same sorted list for prediction
                for mint, entry_data in sorted_alert_items: # Use the sorted list here
                    if not isinstance(entry_data, dict) or "last_alert_timestamp" not in entry_data:
                        continue 
                    
                    last_alert_ts = int(entry_data["last_alert_timestamp"])
                    needs_new_prediction = entry_data.get("runner_prediction_for_alert_at") != last_alert_ts

                    if needs_new_prediction:
                        # Only predict if not recently labeled (labeling implies a prediction isn't needed immediately after)
                        # Or if you always want a prediction regardless of recent labeling, remove this check.
                        # For now, let's assume if it was just eligible for labeling, we might not need an immediate new prediction
                        # if not (eligible_for_labeling and needs_new_label and entry_data.get("runner_label_for_alert_at") == last_alert_ts):

                        log_message(f"Token {mint[:6]}... needs new prediction for alert at {last_alert_ts}.")
                        alert_time_features = extract_alert_time_features(mint, entry_data)
                        if alert_time_features:
                            prediction_result = predict_runner(alert_time_features)
                            if prediction_result:
                                score, p_label = prediction_result
                                # Update the original alert_history dictionary
                                alert_history[mint]["runner_prediction_score"] = score
                                alert_history[mint]["runner_prediction"] = p_label
                                alert_history[mint]["runner_predicted_at"] = current_time_unix
                                alert_history[mint]["runner_prediction_for_alert_at"] = last_alert_ts
                                history_modified_this_run = True
                                log_message(f"  -> Predicted for {mint[:6]}... Score: {score:.3f}, Label: {p_label}")
                        else:
                             log_message(f"Could not extract features for {mint} during prediction.")
            else:
                log_message("No model available for predictions in this cycle.")

            if history_modified_this_run:
                save_json_data(HISTORY_FILE, alert_history) # Save the modified dictionary
                log_message("Alert history updated with analysis results.")

            _AUX_FEATURE_CACHE.clear() 
            log_message(f"--- Analysis cycle complete. Sleeping for {LOOP_SLEEP_SECONDS // 60} minutes. ---")
            time.sleep(LOOP_SLEEP_SECONDS)

        except KeyboardInterrupt:
            log_message("--- Runner Analyzer stopping due to user interrupt ---")
            break
        except Exception as e:
            log_message(f"Unhandled error in main analysis loop: {e}", is_error=True)
            log_message(f"Retrying in {LOOP_SLEEP_SECONDS} seconds...")
            time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    # Ensure necessary directories exist
    os.makedirs(REPORTS_ROOT, exist_ok=True)
    
    # Initialize FEATURE_COLUMNS_ORDER at the start
    FEATURE_COLUMNS_ORDER = get_feature_columns()

    # Initialize API modules that might need it (e.g. cache_manager)
    # birdeye_api.py already calls cache_manager.init_db() on import
    
    main_analysis_loop()