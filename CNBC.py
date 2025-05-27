# --- START OF FILE CNBC_Fetcher.py ---
import requests
import pandas as pd
from datetime import datetime, timezone
import json
import boto3
import decimal
import os
import time # For potential retries or waits

# --- Configuration from Environment Variables ---
SYMBOL_TO_FETCH = os.environ.get('SYMBOL_TO_FETCH', 'US10YTIP') # e.g., US10YTIP, US10Y
TIME_RANGE_TO_FETCH = os.environ.get('TIME_RANGE_TO_FETCH', '1D') # e.g., 1D, 5D, 1Y, 5Y
METRIC_ID_PREFIX = os.environ.get('METRIC_ID_PREFIX', 'CNBC') # e.g., CNBC
METRIC_NAME_SUFFIX = os.environ.get('METRIC_NAME_SUFFIX', 'Close') # e.g., Close, Rate
UNIT_FOR_METRIC = os.environ.get('UNIT_FOR_METRIC', '%') # e.g., %, basis_points

# Construct the dynamic Metric ID for DynamoDB
# Example: CNBC_US10YTIP_1D_Close or CNBC_US10Y_5Y_Rate
METRIC_ID_FOR_DYNAMODB = f"{METRIC_ID_PREFIX}_{SYMBOL_TO_FETCH}_{TIME_RANGE_TO_FETCH}_{METRIC_NAME_SUFFIX}"

# --- DynamoDB Setup ---
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'RealTimeChartData')
# Ensure Boto3 uses the correct region, especially if Lambda/Fargate and DynamoDB are in different regions
# If running in Fargate in the same region as DynamoDB, this often works by default.
# For explicit control: boto3.resource('dynamodb', region_name='your-region')
try:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
except Exception as e:
    print(f"Error initializing DynamoDB resource: {e}")
    # If this fails, the script likely can't proceed.
    # Consider exiting or more robust error handling for a production system.
    raise

def store_data_in_dynamodb(df, metric_id_to_store, unit):
    """
    Processes the DataFrame and stores each data point in DynamoDB.
    """
    if df is None or df.empty:
        print(f"DataFrame for {metric_id_to_store} is None or empty, nothing to store.")
        return 0

    items_stored_count = 0
    # Ensure the table resource is valid
    if table is None:
        print("DynamoDB table object is not initialized. Cannot store data.")
        return 0

    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            datetime_obj = row['DateTime']
            # Ensure datetime_obj is timezone-aware (UTC) before timestamping if it's naive
            if datetime_obj.tzinfo is None:
                datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)
            db_timestamp = int(datetime_obj.timestamp() * 1000)

            try:
                # Ensure the value is not NaN or Inf, which Decimal cannot handle
                if pd.isna(row['Value']) or not pd.Series(row['Value']).is_finite().all():
                    print(f"Skipping row due to non-finite value: {row['Value']} for {metric_id_to_store} at {datetime_obj}")
                    continue
                value = decimal.Decimal(str(row['Value']))
            except (ValueError, TypeError, decimal.InvalidOperation) as e:
                print(f"Could not convert value '{row['Value']}' to Decimal for {metric_id_to_store} at {datetime_obj}. Error: {e}")
                continue

            source_date_str = datetime_obj.strftime('%Y-%m-%d %H:%M:%S %Z') # Include timezone

            item = {
                'metricId': metric_id_to_store,
                'timestamp': db_timestamp, # Primary Sort Key
                'value': value,
                'sourceDate': source_date_str,
                'unit': unit
            }

            batch.put_item(Item=item)
            items_stored_count += 1

    print(f"Successfully prepared {items_stored_count} items for metric '{metric_id_to_store}' for DynamoDB batch write.")
    return items_stored_count

def fetch_cnbc_data(symbol_to_fetch, time_range):
    """
    Fetches historical data for a given symbol from CNBC for a given time range.
    Returns a DataFrame with 'DateTime' and 'Value' columns.
    """
    base_url = "https://webql-redesign.cnbcfm.com/graphql"
    variables_payload = {"symbol": symbol_to_fetch, "timeRange": time_range}
    extensions_payload = {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "9e1670c29a10707c417a1efd327d4b2b1d456b77f1426e7e84fb7d399416bb6b"
        }
    }
    params = {
        "operationName": "getQuoteChartData",
        "variables": json.dumps(variables_payload),
        "extensions": json.dumps(extensions_payload)
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.cnbc.com/',
        'Origin': 'https://www.cnbc.com'
    }

    print(f"Fetching CNBC data for symbol: {symbol_to_fetch}, time range: {time_range}")
    max_retries = 3
    retry_delay = 5 # seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            if (data.get("data") and
                isinstance(data["data"], dict) and
                data["data"].get("chartData") and
                isinstance(data["data"]["chartData"], dict) and
                data["data"]["chartData"].get("priceBars")):

                price_bars = data["data"]["chartData"]["priceBars"]
                processed_data = []

                if not price_bars:
                    print(f"Price bars data received for {symbol_to_fetch} ({time_range}), but it was empty.")
                    return pd.DataFrame(processed_data, columns=['DateTime', 'Value'])

                for bar in price_bars:
                    try:
                        timestamp_val = bar.get("tradeTimeinMills")
                        if timestamp_val is None:
                            print(f"Skipping a bar due to missing 'tradeTimeinMills'. Bar data: {bar}")
                            continue

                        timestamp_ms = int(timestamp_val)
                        # Ensure UTC for consistency
                        dt_object = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

                        close_value_str = str(bar.get("close", "0"))

                        try:
                            # CNBC sometimes returns "UNCH", handle this by trying to convert and skipping if error
                            # Or if it returns percentage strings
                            if '%' in close_value_str:
                                value_to_store = float(close_value_str.replace('%', ''))
                            else:
                                value_to_store = float(close_value_str)
                        except ValueError:
                            print(f"Skipping bar due to non-numeric close value: '{close_value_str}'. Symbol: {symbol_to_fetch}, Bar: {bar}")
                            continue

                        processed_data.append({
                            "DateTime": dt_object, # This is a datetime object
                            "Value": value_to_store
                        })
                    except (ValueError, TypeError) as e_bar:
                        print(f"Skipping a bar for {symbol_to_fetch} due to data conversion error: {e_bar}. Bar data: {bar}")
                        continue

                df = pd.DataFrame(processed_data)
                if not df.empty:
                    df.sort_values(by='DateTime', inplace=True) # Ensure data is sorted by time
                return df
            else:
                print(f"Could not find 'priceBars' structure for {symbol_to_fetch} ({time_range}). Response data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
                if data and data.get("data") and isinstance(data["data"], dict):
                     print(f"data['chartData'] keys: {data['data']['chartData'].keys() if data['data'].get('chartData') and isinstance(data['data']['chartData'], dict) else 'chartData not found or not a dict'}")
                return pd.DataFrame(columns=['DateTime', 'Value']) # Return empty DF for structure issues

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error for {symbol_to_fetch} ({time_range}): {http_err}. Attempt {attempt + 1} of {max_retries}.")
            if attempt + 1 == max_retries: return pd.DataFrame(columns=['DateTime', 'Value'])
        except requests.exceptions.RequestException as e:
            print(f"Request error for {symbol_to_fetch} ({time_range}): {e}. Attempt {attempt + 1} of {max_retries}.")
            if attempt + 1 == max_retries: return pd.DataFrame(columns=['DateTime', 'Value'])
        except json.JSONDecodeError as e_json:
            print(f"JSON decode error for {symbol_to_fetch} ({time_range}): {e_json}. Response text: {response.text if 'response' in locals() else 'No response text'}. Attempt {attempt + 1} of {max_retries}.")
            if attempt + 1 == max_retries: return pd.DataFrame(columns=['DateTime', 'Value'])
        except Exception as e_proc:
            print(f"Unexpected error processing {symbol_to_fetch} ({time_range}): {e_proc}. Attempt {attempt + 1} of {max_retries}.")
            if attempt + 1 == max_retries: return pd.DataFrame(columns=['DateTime', 'Value'])
        time.sleep(retry_delay) # Wait before retrying
    return pd.DataFrame(columns=['DateTime', 'Value']) # Should be unreachable if loop completes


if __name__ == "__main__":
    print(f"--- Running CNBC Generic Data Scraper ---")
    print(f"Target Symbol: {SYMBOL_TO_FETCH}, Time Range: {TIME_RANGE_TO_FETCH}")
    print(f"Target DynamoDB Metric ID: {METRIC_ID_FOR_DYNAMODB}")
    print(f"Target DynamoDB Table: {DYNAMODB_TABLE_NAME}")
    print(f"Metric Unit: {UNIT_FOR_METRIC}")

    # Ensure DynamoDB table object is valid before proceeding
    if 'table' not in globals() or table is None:
        print("DynamoDB table could not be initialized. Exiting.")
        exit(1) # Exit if DynamoDB isn't set up

    df_data = fetch_cnbc_data(symbol_to_fetch=SYMBOL_TO_FETCH, time_range=TIME_RANGE_TO_FETCH)

    if df_data is not None and not df_data.empty:
        print(f"Successfully fetched {len(df_data)} data points for {SYMBOL_TO_FETCH} ({TIME_RANGE_TO_FETCH}).")

        print(f"\nStoring fetched {METRIC_ID_FOR_DYNAMODB} data into DynamoDB...")
        items_written = store_data_in_dynamodb(df_data, METRIC_ID_FOR_DYNAMODB, UNIT_FOR_METRIC)
        print(f"DynamoDB storage process completed. {items_written} items for {METRIC_ID_FOR_DYNAMODB} processed.")

    elif df_data is not None and df_data.empty:
        print(f"Fetched data for {SYMBOL_TO_FETCH} ({TIME_RANGE_TO_FETCH}), but it resulted in an empty DataFrame (no processable price bars or API returned no data).")
    else: # df_data is None (meaning an error occurred during fetch or initial processing)
        print(f"Failed to fetch or process data for {SYMBOL_TO_FETCH} ({TIME_RANGE_TO_FETCH}). Check logs for errors.")

    print(f"--- CNBC Generic Data Scraper Finished ---")

# --- END OF FILE CNBC_Fetcher.py ---