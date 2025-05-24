# You would run this in your own Python environment
# pip install selenium pandas webdriver-manager boto3

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# To manage ChromeDriver installation automatically
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
import time
import json

# --- New Imports for DynamoDB ---
import boto3
import decimal # For accurate number storage in DynamoDB
from datetime import datetime, timezone # For timestamp conversion
import os # To get environment variables for table name

# --- DynamoDB Configuration ---
# It's good practice to get the table name from an environment variable
# When running in Fargate, we'll set this environment variable.
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'RealTimeChartData') # Default for local testing

# Initialize DynamoDB client (Boto3 will use credentials from the Fargate Task Role)
# Ensure AWS credentials and region are configured in your environment
# For local testing, this might be via ~/.aws/credentials or environment variables
# For Fargate, this will be handled by the task role.
try:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    print(f"DynamoDB resource initialized. Target table: {DYNAMODB_TABLE_NAME}")
except Exception as e:
    print(f"Error initializing DynamoDB resource: {e}")
    dynamodb = None
    table = None
# --- End DynamoDB Configuration ---


def fetch_sofr_strip_rates_with_selenium():
    """
    Attempts to fetch SOFR strip rates from CME Group using Selenium
    and parses the specific JSON structure provided.
    """
    current_timestamp = int(time.time() * 1000)
    url = f"https://www.cmegroup.com/services/sofr-strip-rates/?isProtected&_t={current_timestamp}"

    # --- WebDriver Setup ---
    try:
        # Use webdriver_manager (install with: pip install webdriver-manager)
        service = ChromeService(ChromeDriverManager().install())
        
        # Optional: Add headless mode to run without opening a browser window
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu') # Recommended for headless
        chrome_options.add_argument('--window-size=1920,1080') # Can help with some headless issues
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")


        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("WebDriver Manager successfully set up ChromeDriver (headless mode).")
    except Exception as e_manager:
        print(f"Error setting up ChromeDriver with WebDriver Manager: {e_manager}")
        return pd.DataFrame()

    extracted_data_df = pd.DataFrame()
    page_content_for_debug = "" # Initialize for debugging

    try:
        print(f"Navigating to URL: {url}")
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        page_content_for_debug = driver.page_source # For debugging if JSON parsing fails

        try:
            pre_element = driver.find_element(By.TAG_NAME, "pre")
            json_text = pre_element.text
            print("Found JSON data within a <pre> tag.")
        except:
            body_element = driver.find_element(By.TAG_NAME, "body")
            json_text = body_element.text
            print("Did not find a <pre> tag, using body text directly.")
        
        print(f"Attempting to parse JSON content (length: {len(json_text)} characters).")
        data = json.loads(json_text)
        
        processed_data = []
        if isinstance(data, dict) and 'resultsStrip' in data:
            daily_strips = data['resultsStrip']
            
            for i, day_data in enumerate(daily_strips):
                if i >= 5: # Only process the last 5 available days
                    break
                
                date_str = day_data.get('date')
                
                overnight_rate_str = str(day_data.get('overnight', 'N/A'))
                overnight_rate = None
                if overnight_rate_str.lower() not in ['-', 'n/a', 'none', '']:
                    try:
                        overnight_rate = float(overnight_rate_str)
                    except ValueError:
                        overnight_rate = None # Or keep as 'N/A' text if you prefer

                avg_30d = day_data.get('average30day')
                avg_90d = day_data.get('average90day')
                avg_180d = day_data.get('average180day')
                sofr_index = day_data.get('index')

                term_rates = {'1M': None, '3M': None, '6M': None, '1Y': None} # Initialize
                if 'rates' in day_data and 'sofrRatesFixing' in day_data['rates']:
                    rates_by_term = {}
                    for rate_info_item in day_data['rates']['sofrRatesFixing']:
                        term = rate_info_item.get('term')
                        if term:
                            if term not in rates_by_term:
                                rates_by_term[term] = []
                            rates_by_term[term].append(rate_info_item)
                    
                    for term_key, L_rates_info in rates_by_term.items():
                        if term_key not in term_rates: # Only process expected terms
                            continue

                        selected_rate_info = None
                        if len(L_rates_info) == 1:
                            selected_rate_info = L_rates_info[0]
                        else:
                            # Prioritize by timestamp matching the entry's date
                            on_date_rates = [r for r in L_rates_info if r.get('timestamp','').startswith(date_str)]
                            if on_date_rates:
                                t10_rates = [r for r in on_date_rates if 'T10:00' in r.get('timestamp','')]
                                if t10_rates:
                                    selected_rate_info = t10_rates[0]
                                else:
                                    selected_rate_info = on_date_rates[0]
                            else:
                                selected_rate_info = L_rates_info[0] # Fallback

                        if selected_rate_info and 'price' in selected_rate_info:
                            try:
                                term_rates[term_key] = float(selected_rate_info['price'])
                            except (ValueError, TypeError):
                                pass # Keep as None if conversion fails
                
                processed_data.append({
                    'Date': date_str,
                    'Overnight SOFR': overnight_rate,
                    '1M SOFR': term_rates.get('1M'),
                    '3M SOFR': term_rates.get('3M'),
                    '6M SOFR': term_rates.get('6M'),
                    '1Y SOFR': term_rates.get('1Y'),
                    '30-Day Avg SOFR': avg_30d,
                    '90-Day Avg SOFR': avg_90d,
                    '180-Day Avg SOFR': avg_180d,
                    'SOFR Index': sofr_index
                })
            
            if processed_data:
                df = pd.DataFrame(processed_data)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                extracted_data_df = df
            else:
                print("Processed data list is empty (no items in 'resultsStrip' or loop limit hit).")

        else:
            print("Could not find 'resultsStrip' key in JSON. Full data dump for inspection:")
            print(json.dumps(data, indent=2))
            
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Selenium: {e}")
        print("Received text that was not valid JSON. Page content snippet:")
        print(page_content_for_debug[:1000])
    except Exception as e:
        print(f"An error occurred with Selenium: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'driver' in locals() and driver:
            driver.quit()
            print("WebDriver closed.")
            
    return extracted_data_df

# --- DynamoDB Helper Function ---
def convert_date_to_utc_timestamp_ms(date_obj):
    """Converts a pandas Timestamp or datetime object to a UTC Unix timestamp in milliseconds.
       Assumes the date represents the "as of" date, pegged to midnight UTC.
    """
    if pd.isna(date_obj):
        return None
    dt_obj = pd.to_datetime(date_obj) # Ensure it's a pandas Timestamp/datetime
    # Make it timezone-aware at UTC, assuming the date is effectively a UTC date label
    dt_obj_utc = dt_obj.tz_localize('UTC') if dt_obj.tzinfo is None else dt_obj.tz_convert('UTC')
    return int(dt_obj_utc.timestamp() * 1000)

# --- DynamoDB Storage Function ---
def store_sofr_data_in_dynamodb(df):
    """
    Processes the SOFR DataFrame and stores each relevant metric
    as a separate item in DynamoDB.
    """
    if df.empty:
        print("DataFrame is empty, nothing to store in DynamoDB.")
        return 0 # Return count of items stored
    
    if table is None:
        print("DynamoDB table is not initialized. Cannot store data.")
        return 0

    # Define a mapping from DataFrame columns to metricId and unit
    metric_mapping = {
        'Overnight SOFR': {'metricId': 'SOFR_Overnight', 'unit': '%'},
        '1M SOFR': {'metricId': 'SOFR_1M_Term', 'unit': '%'},
        '3M SOFR': {'metricId': 'SOFR_3M_Term', 'unit': '%'},
        '6M SOFR': {'metricId': 'SOFR_6M_Term', 'unit': '%'},
        '1Y SOFR': {'metricId': 'SOFR_1Y_Term', 'unit': '%'},
        '30-Day Avg SOFR': {'metricId': 'SOFR_30D_Avg', 'unit': '%'},
        '90-Day Avg SOFR': {'metricId': 'SOFR_90D_Avg', 'unit': '%'},
        '180-Day Avg SOFR': {'metricId': 'SOFR_180D_Avg', 'unit': '%'},
        'SOFR Index': {'metricId': 'SOFR_Index', 'unit': None} # No unit for index
    }

    items_stored_count = 0
    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            date_obj = row['Date']
            db_timestamp = convert_date_to_utc_timestamp_ms(date_obj)

            if db_timestamp is None:
                print(f"Skipping row with invalid date: {row['Date']}")
                continue

            source_date_str = pd.to_datetime(date_obj).strftime('%Y-%m-%d')

            for column_name, metric_info in metric_mapping.items():
                if column_name in row and not pd.isna(row[column_name]):
                    try:
                        # Convert value to string first, then to Decimal, for robustness
                        value_str = str(row[column_name])
                        value = decimal.Decimal(value_str)
                    except (ValueError, TypeError, decimal.InvalidOperation) as e:
                        print(f"Could not convert value '{row[column_name]}' to Decimal for {metric_info['metricId']} on {source_date_str}. Error: {e}")
                        continue

                    item = {
                        'metricId': metric_info['metricId'],
                        'timestamp': db_timestamp, # This is already a number
                        'value': value,
                        'sourceDate': source_date_str
                    }
                    if metric_info['unit']:
                        item['unit'] = metric_info['unit']
                    
                    # print(f"Preparing to store: {item}") # Uncomment for debugging
                    batch.put_item(Item=item)
                    items_stored_count += 1
    
    print(f"Successfully prepared {items_stored_count} items for DynamoDB batch write.")
    return items_stored_count


# --- Main Execution Block ---
if __name__ == '__main__':
    print("Fetching SOFR strip rates from CME Group using Selenium...")
    
    sofr_data_selenium_df = fetch_sofr_strip_rates_with_selenium()

    if not sofr_data_selenium_df.empty:
        print("\nLast 5 Days of SOFR Strip Rates (via Selenium):")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200) # Adjust width as needed for your console
        pd.set_option('display.float_format', '{:.5f}'.format) # Format float precision
        print(sofr_data_selenium_df)

        # --- Store data in DynamoDB ---
        if table: # Check if table was initialized successfully
            print("\nStoring fetched SOFR data into DynamoDB...")
            items_written = store_sofr_data_in_dynamodb(sofr_data_selenium_df)
            print(f"Data storage process completed. {items_written} items processed for DynamoDB.")
        else:
            print("\nSkipping DynamoDB storage because table was not initialized.")
        # --- End Store data in DynamoDB ---

    else:
        print("\nCould not retrieve or parse the SOFR strip rates data using Selenium.")

    print("\n--- Important Notes for Selenium ---")
    print("1. WebDriver: Script uses webdriver-manager for automatic ChromeDriver setup.")
    print("2. Headless Mode: Enabled by default for convenience.")
    print("3. Data Structure: Parsing is specific to the observed JSON from 'resultsStrip'.")
    print("4. Website Changes: If CME Group changes its website structure or JSON format, this script may need updates.")
    print("\n--- Important Notes for DynamoDB ---")
    print(f"5. DynamoDB Table: Data is intended for table '{DYNAMODB_TABLE_NAME}'.")
    print("6. AWS Credentials: Ensure AWS credentials and region are configured (e.g., via IAM role for Fargate, or local AWS CLI config).")
    print("7. Table Schema: Assumes 'metricId' (String, HASH), 'timestamp' (Number, RANGE).")
