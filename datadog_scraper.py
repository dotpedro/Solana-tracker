import asyncio
import json
from datetime import datetime, timezone, time as dt_time
from typing import Dict, Any, Optional, Tuple, List

from playwright.async_api import async_playwright, Page, Locator

# --- Configuration (remains the same) ---
BASE_DATADOG_URL = "https://p.us5.datadoghq.com/sb/339e0590-c5d4-11ed-9c7b-da7ad0900005-9a1e3a7c98f885976f7b54dbf67b9a3f"
URL_PARAMS = {
    "fromUser": "false",
    "refresh_mode": "sliding", # This might be overridden by live=true later by DD
    "tpl_var_apiKey[0]": "4edf131a-6d04-4123-a65d-e0ed03e63029",
    "tpl_var_method[0]": "*",
    "tpl_var_network[0]": "mainnet",
    "tpl_var_projectId[0]": "a0311864-9458-4725-ac41-b1dfb88aa15a",
    "live": "false" # We want to control the time range
}

# --- Helper Functions (parse_credits_from_locator_text, get_value_from_locator, get_today_so_far_timestamps_ms - remain the same) ---
def get_today_so_far_timestamps_ms() -> Tuple[int, int]:
    now_utc = datetime.now(timezone.utc)
    start_of_today_utc = datetime.combine(now_utc.date(), dt_time.min, tzinfo=timezone.utc)
    from_ts_ms = int(start_of_today_utc.timestamp() * 1000)
    to_ts_ms = int(now_utc.timestamp() * 1000)
    return from_ts_ms, to_ts_ms

async def parse_credits_from_locator_text(s: Optional[str], context_msg: str = "") -> int:
    if not s or s.strip() == "-" or s.strip().lower() == "no data":
        # print(f"Debug: Parsing '{s}' as 0 credits due to empty/no data. Context: {context_msg}")
        return 0

    s_cleaned = s.strip().replace(",", "")
    
    try:
        num_part = s_cleaned
        multiplier = 1
        
        if s_cleaned.lower().endswith('k'):
            num_part = s_cleaned[:-1]
            multiplier = 1000
        elif s_cleaned.lower().endswith('m'):
            num_part = s_cleaned[:-1]
            multiplier = 1_000_000
        elif s_cleaned.lower().endswith('b'):
            num_part = s_cleaned[:-1]
            multiplier = 1_000_000_000
        
        parsed_val = int(float(num_part) * multiplier)
        # print(f"Debug: Parsed '{s}' to {parsed_val}. Context: {context_msg}")
        return parsed_val
    except ValueError:
        print(f"Warning: Could not parse '{s}' as a number. Context: {context_msg}. Returning 0.")
        return 0

async def get_value_from_locator(locator: Locator, context_msg: str = "") -> int:
    value_from_title = await locator.get_attribute('title')
    if value_from_title and value_from_title.strip() and value_from_title.strip().lower() != "no data":
        # print(f"DEBUG (get_value_from_locator): Using title attribute: '{value_from_title}' for {context_msg}")
        return await parse_credits_from_locator_text(value_from_title, f"(from title attr) {context_msg}")

    value_from_text = await locator.inner_text()
    # print(f"DEBUG (get_value_from_locator): Using inner_text: '{value_from_text}' for {context_msg}")
    return await parse_credits_from_locator_text(value_from_text, f"(from inner_text) {context_msg}")

# --- scrape_datadog_dashboard function from previous correct answer (targeting "Successful Requests" and "Overall Requests")
# This function should be correct if the page loads properly.
async def scrape_datadog_dashboard(page: Page) -> Dict[str, Any]:
    output_data: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_credits": 0, # Will come from "Successful Requests"
        "methods": {}       # Will come from "Overall Requests"
    }
    
    # Wait for a known widget title (using h3 as per your HTML sample)
    # This is now handled in main() before calling this function.
    # We'll assume the page structure is ready when this function is called.

    # --- 1. Scrape "Successful Requests" (as Total Credits) ---
    print("Scraping 'Successful Requests' for total_credits...")
    try:
        # Updated to target h3 with widget__title-text if that's the final rendered title tag
        widget_base_locator = page.locator(
            'div.widget-container:has(h3.widget__title-text:has-text("Successful Requests"))'
        )
        # Fallback if the h2 structure is used
        if not await widget_base_locator.count():
            widget_base_locator = page.locator(
                'div.widget-container:has(h2.widget-title:has-text("Successful Requests"))'
            )
        
        await widget_base_locator.wait_for(state="visible", timeout=20000)
        
        # Wait for this specific widget to signal it has data
        # The "has-data" class is a good indicator, but sometimes the value populates even later.
        await widget_base_locator.locator('div.widget.has-data').wait_for(state="visible", timeout=30000)
        
        # More specific locator for the value based on common Datadog "Query Value" widgets
        # and your provided HTML (query-value__value for the number, title attribute often has raw value)
        value_locator = widget_base_locator.locator('div.query-value__value')
        
        if await value_locator.count() > 0:
            # The get_value_from_locator tries 'title' attribute of the span first, then inner_text
            raw_text_value = await value_locator.first().inner_text() # Get the "19.13k"
            print(f"  Raw text for Successful Requests: '{raw_text_value}'")
            output_data["total_credits"] = await parse_credits_from_locator_text(raw_text_value, "Successful Requests value")
            
            # Attempt to get from title if parsing inner text failed or was 0
            if output_data["total_credits"] == 0 and raw_text_value.strip() != "0": # to avoid re-parsing if it was a legit 0
                title_val = await value_locator.first().get_attribute('title')
                if title_val:
                    print(f"  Retrying with title attribute for Successful Requests: '{title_val}'")
                    credits_from_title = await parse_credits_from_locator_text(title_val, "Successful Requests title attr")
                    if credits_from_title != 0: # Prefer non-zero from title if inner_text parsing failed
                        output_data["total_credits"] = credits_from_title

            print(f"  Successfully scraped total_credits (from Successful Requests): {output_data['total_credits']}")
        else:
            print("  Warning: 'Successful Requests' value element (div.query-value__value) not found within widget.")
            await page.screenshot(path="error_screenshot_successful_requests_value_not_found.png")

    except Exception as e:
        print(f"  Error scraping 'Successful Requests': {e}")
        await page.screenshot(path="error_screenshot_successful_requests_exception.png")

    # --- 2. Scrape "Overall Requests" (as Per-Method Credits) ---
    print("Scraping 'Overall Requests' for per-method credits...")
    try:
        widget_base_locator = page.locator(
             'div.widget-container:has(h3.widget__title-text:has-text("Overall Requests"))'
        )
        if not await widget_base_locator.count():
             widget_base_locator = page.locator(
                 'div.widget-container:has(h2.widget-title:has-text("Overall Requests"))'
        )
        await widget_base_locator.wait_for(state="visible", timeout=20000)
        await widget_base_locator.locator('div.widget.has-data').wait_for(state="visible", timeout=30000)
        
        # Based on your HTML, this is a "stacked-toplist"
        # Rows are typically 'div.stacked-toplist__item'
        rows_locator = widget_base_locator.locator('div.stacked-toplist__item')
        row_count = await rows_locator.count()
        
        if row_count > 0:
            print(f"  Found {row_count} method rows in 'Overall Requests'. Processing...")
            for i in range(row_count):
                row = rows_locator.nth(i)
                try:
                    # Value: 'span.stacked-toplist__value-label'
                    value_span_locator = row.locator('span.stacked-toplist__value-label')
                    raw_method_credits_text = await value_span_locator.inner_text()
                    method_credits = await parse_credits_from_locator_text(raw_method_credits_text, f"Overall Requests - Row {i} - Value")
                    
                    # Method Name (Label): 'svg.stacked-toplist__label-svg text'
                    # Or more robustly, the data attribute on the svg
                    method_name_svg_locator = row.locator('svg.stacked-toplist__label-svg')
                    method_name = await method_name_svg_locator.get_attribute('data-synthetics-toplist-label')
                    
                    if not method_name: # Fallback if data attribute is not present
                        method_name_text_locator = method_name_svg_locator.locator('text.stacked-toplist__label-text')
                        method_name = (await method_name_text_locator.inner_text()).strip()

                    if method_name:
                        output_data["methods"][method_name] = method_credits
                        print(f"    Scraped: {method_name} = {method_credits}")
                    else:
                        print(f"    Warning: Could not extract method name for row {i} in 'Overall Requests'. Raw credits text: '{raw_method_credits_text}'")
                except Exception as row_e:
                    print(f"    Error processing a row in 'Overall Requests' (row {i}): {row_e}")
        else:
            print("  Warning: 'Overall Requests' rows (div.stacked-toplist__item) not found or widget is empty.")
            await page.screenshot(path="error_screenshot_overall_requests_rows_not_found.png")

    except Exception as e:
        print(f"  Error scraping 'Overall Requests': {e}")
        await page.screenshot(path="error_screenshot_overall_requests_exception.png")
        
    return output_data

async def main():
    from_ts, to_ts = get_today_so_far_timestamps_ms()
    
    current_params = URL_PARAMS.copy()
    current_params["from_ts"] = str(from_ts)
    current_params["to_ts"] = str(to_ts)
    
    query_string_parts: List[str] = [f"{k}={v}" for k, v in current_params.items()]
    query_string = "&".join(query_string_parts)
    target_url = f"{BASE_DATADOG_URL}?{query_string}"
    
    print(f"--- Datadog Scraper Initializing (Debugging Load State) ---")
    print(f"Targeting URL: {target_url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=1000) # Keep headless=False, slow_mo
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        page.set_default_timeout(120000) # Generous default timeout

        try:
            print(f"Navigating to Datadog dashboard: {target_url}")
            await page.goto(target_url, wait_until="networkidle", timeout=120000) # Try 'networkidle'
            print("Initial page navigation complete (waited for 'networkidle' event).")

            print("Taking a screenshot of the loaded page state BEFORE looking for specific widgets...")
            await page.screenshot(path="loaded_page_state_before_widget_search.png")
            print("Screenshot 'loaded_page_state_before_widget_search.png' saved.")

            # Print the full page HTML content at this point for manual inspection
            page_content = await page.content()
            with open("loaded_page_content.html", "w", encoding="utf-8") as f:
                f.write(page_content)
            print("Full page HTML content saved to 'loaded_page_content.html'.")
            print("Please inspect this HTML and the screenshot to verify widget titles and structure.")

            # --- AT THIS POINT, PAUSE THE SCRIPT IF RUNNING NON-HEADLESS ---
            # --- AND MANUALLY INSPECT THE BROWSER WINDOW!           ---
            if not browser.is_connected: # Check if browser is still connected
                 print("Browser closed unexpectedly.")
                 return

            print("Manually inspect the browser window now. Press Enter in this console to continue...")
            # input() # Uncomment this line if you want to manually pause the script here


            # Original logic to find widget titles (will likely still fail, but for completeness)
            print("Attempting to find key dashboard widget titles...")
            successful_requests_title_loc = page.locator(
                'div.widget-container:has(h3.widget__title-text:text-is("Successful Requests")), '
                'div.widget-container:has(h2.widget-title:text-is("Successful Requests"))'
            ).first
            
            overall_requests_title_loc = page.locator(
                'div.widget-container:has(h3.widget__title-text:text-is("Overall Requests")), '
                'div.widget-container:has(h2.widget-title:text-is("Overall Requests"))'
            ).first

            key_widget_visible = False
            try:
                print("Waiting for 'Successful Requests' title...")
                await successful_requests_title_loc.wait_for(state="visible", timeout=30000) # Reduced for this test
                print("Widget title 'Successful Requests' is visible.")
                key_widget_visible = True
            except Exception as e_sr:
                print(f"Could not find 'Successful Requests' title: {e_sr}")
                print("Trying 'Overall Requests' title...")
                try:
                    await overall_requests_title_loc.wait_for(state="visible", timeout=30000) # Reduced
                    print("Widget title 'Overall Requests' is visible.")
                    key_widget_visible = True
                except Exception as e_or:
                    print(f"Could not find 'Overall Requests' title either: {e_or}")
                    raise Exception("Neither key widget title became visible after initial load checks.")
            
            if not key_widget_visible:
                 raise Exception("Key widget titles did not become visible (main check).")

            print("Brief pause after widget title visibility confirmed (if it was)...")
            await page.wait_for_timeout(5000)

            data = await scrape_datadog_dashboard(page) # scrape_datadog_dashboard is the same
            
            print("\n--- Scraped Data (Debugging Load State) ---")
            print(json.dumps(data, indent=2))

            with open("datadog_scraped_data_debug_load.json", "w") as f:
                json.dump(data, f, indent=2)
            print("\nData saved to datadog_scraped_data_debug_load.json")

        except Exception as e:
            print(f"An critical error occurred in the main execution: {e}")
            if 'page' in locals() and page and not page.is_closed():
                 try:
                    await page.screenshot(path="error_screenshot_main_critical_debug_load.png")
                    print("Saved screenshot to error_screenshot_main_critical_debug_load.png")
                 except Exception as ss_err:
                    print(f"Error taking screenshot: {ss_err}")
        finally:
            print("Closing browser...")
            if 'browser' in locals() and browser.is_connected():
                await browser.close()
    print("--- Datadog Scraper Finished (Debugging Load State) ---")

# The scrape_datadog_dashboard, get_today_so_far_timestamps_ms, 
# parse_credits_from_locator_text, and get_value_from_locator functions
# remain the same as in the previous "HTML Informed Targets" version.
# Just ensure you have them in your datadog_scraper.py file.
# For brevity, I'm not re-pasting them here.

if __name__ == "__main__":
    # Keep the initial note
    asyncio.run(main())

if __name__ == "__main__":
    print("NOTE: This script uses Playwright...") # Keep the initial note
    asyncio.run(main())