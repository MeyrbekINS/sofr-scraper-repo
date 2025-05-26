import requests
import pandas as pd
from datetime import datetime
import json
import boto3 # Added
import decimal # Added
import os      # Added

# --- Configuration for this specific script ---
# Decide which time_range this instance of the script will always fetch.
# For somewhat "intraday" data, "1D" (1-minute timeframe) or "5D" (5-minute timeframe) makes sense.
# If you only want daily closing, "1Y" would be appropriate.
# Let's choose "1D" for more frequent updates, assuming the EventBridge schedule will be more frequent.
# If you want different timeframes, you might have multiple scheduled tasks, each running a version
# of this script configured for a different time_range and potentially a different metricId suffix.
TIME_RANGE_TO_FETCH = "1D" 
METRIC_ID_FOR_DYNAMODB = 'CNBC_US10YTIP_Close' 
UNIT_FOR_METRIC = '%' # Assuming the yield is a percentage

# --- DynamoDB Setup ---
# Boto3 will use the Fargate task's region by default if region_name is not specified.
# This assumes your Fargate task and DynamoDB table are in the same region (eu-north-1).
dynamodb = boto3.resource('dynamodb') 
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'RealTimeChartData') 
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def store_yield_data_in_dynamodb(df, metric_id_to_store, unit):
    """
    Processes the yield DataFrame and stores each data point in DynamoDB.
    """
    if df is None or df.empty: # Added check for df being None
        print(f"DataFrame for {metric_id_to_store} is None or empty, nothing to store.")
        return 0

    items_stored_count = 0
    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            datetime_obj = row['DateTime'] 
            db_timestamp = int(datetime_obj.timestamp() * 1000) 

            try:
                value = decimal.Decimal(str(row['Close Price']))
            except (ValueError, TypeError, decimal.InvalidOperation) as e:
                print(f"Could not convert value '{row['Close Price']}' to Decimal for {metric_id_to_store} at {datetime_obj}. Error: {e}")
                continue
            
            # Using the full datetime for sourceDate as it might have time component for 1D/5D
            source_date_str = datetime_obj.strftime('%Y-%m-%d %H:%M:%S') 

            item = {
                'metricId': metric_id_to_store,
                'timestamp': db_timestamp,
                'value': value,
                'sourceDate': source_date_str,
                'unit': unit
            }
            
            batch.put_item(Item=item)
            items_stored_count += 1
    
    print(f"Successfully prepared {items_stored_count} items for metric '{metric_id_to_store}' for DynamoDB batch write.")
    return items_stored_count

def fetch_cnbc_data(symbol_to_fetch, time_range): # Added symbol_to_fetch as parameter
    """
    Fetches historical data for a given symbol from CNBC for a given time range.
    """
    base_url = "https://webql-redesign.cnbcfm.com/graphql"
    
    variables_payload = {
        "symbol": symbol_to_fetch, # Use the passed symbol
        "timeRange": time_range
    }
    
    extensions_payload = {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "9e1670c29a10707c417a1efd327d4b2b1d456b77f1426e7e84fb7d399416bb6b" # This hash might be generic
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
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=30) # Added timeout
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
                return pd.DataFrame(processed_data)

            for bar in price_bars:
                try:
                    timestamp_val = bar.get("tradeTimeinMills")
                    if timestamp_val is None:
                        print(f"Skipping a bar due to missing 'tradeTimeinMills'. Bar data: {bar}")
                        continue
                    
                    timestamp_ms = int(timestamp_val)
                    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
                    
                    close_value_str = str(bar.get("close", "0"))
                    
                    # CNBC sometimes returns "UNCH" or other non-numeric strings for close when market is closed or data is pending
                    # We need to handle this gracefully.
                    try:
                        if '%' in close_value_str:
                            close_price = float(close_value_str.replace('%', ''))
                        else:
                            # Attempt to convert to float, if it fails, it's not a valid number
                            close_price = float(close_value_str)
                    except ValueError:
                        print(f"Skipping bar due to non-numeric close value: '{close_value_str}'. Bar data: {bar}")
                        continue # Skip this bar if close price isn't a valid number
                    
                    processed_data.append({
                        "DateTime": dt_object,
                        "Close Price": close_price
                    })
                except (ValueError, TypeError) as e_bar:
                    print(f"Skipping a bar due to data conversion error: {e_bar}. Bar data: {bar}")
                    continue
            
            df = pd.DataFrame(processed_data)
            return df
        else:
            print(f"Could not find 'priceBars' structure for {symbol_to_fetch} ({time_range}). Full response data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            if data and data.get("data") and isinstance(data["data"], dict):
                 print(f"data['chartData'] keys: {data['data']['chartData'].keys() if data['data'].get('chartData') and isinstance(data['data']['chartData'], dict) else 'chartData not found or not a dict'}")
            return None # Return None for structure issues

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error for {symbol_to_fetch} ({time_range}): {http_err}")
        print(f"Response content: {response.content.decode(errors='ignore') if response else 'No response content'}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for {symbol_to_fetch} ({time_range}): {e}")
        return None
    except json.JSONDecodeError as e_json:
        print(f"JSON decode error for {symbol_to_fetch} ({time_range}): {e_json}. Response text: {response.text if response else 'No response text'}")
        return None
    except Exception as e_proc:
        print(f"Unexpected error processing {symbol_to_fetch} ({time_range}): {e_proc}")
        return None

if __name__ == "__main__":
    print(f"--- Running CNBC US10YTIP Scraper ---")
    print(f"Fetching data for symbol: US10YTIP, Time range: {TIME_RANGE_TO_FETCH}")
    
    df_tips_data = fetch_cnbc_data(symbol_to_fetch="US10YTIP", time_range=TIME_RANGE_TO_FETCH)
    
    if df_tips_data is not None and not df_tips_data.empty:
        print(f"Successfully fetched {len(df_tips_data)} data points for US10YTIP ({TIME_RANGE_TO_FETCH}).")
        
        print(f"\nStoring fetched {METRIC_ID_FOR_DYNAMODB} data into DynamoDB...")
        items_written = store_yield_data_in_dynamodb(df_tips_data, METRIC_ID_FOR_DYNAMODB, UNIT_FOR_METRIC)
        print(f"DynamoDB storage process completed. {items_written} items for {METRIC_ID_FOR_DYNAMODB} processed.")

        # Optional: Display last few entries for local testing verification
        # print("\nLast 7 Entries (for verification):")
        # display_df = df_tips_data.copy()
        # display_df['Timestamp'] = display_df["DateTime"].dt.strftime('%Y-%m-%d %H:%M')
        # last_entries_df = display_df[['Timestamp', 'Close Price']].tail(7)
        # if not last_entries_df.empty:
        #     last_entries_df['Close Price'] = last_entries_df['Close Price'].map('{:.4f}'.format)
        # print(last_entries_df.to_string(index=False))

    elif df_tips_data is not None and df_tips_data.empty: # df_data is an empty DataFrame
        print(f"Fetched data for US10YTIP ({TIME_RANGE_TO_FETCH}), but it resulted in an empty DataFrame (no processable price bars).")
    else: # df_data is None (meaning an error occurred during fetch or initial processing)
        print(f"Failed to fetch or process data for US10YTIP ({TIME_RANGE_TO_FETCH}). Check logs for errors.")
    
    print(f"--- CNBC US10YTIP Scraper Finished ---")