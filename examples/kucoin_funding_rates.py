import ccxt
import csv
import os
import io
import re

def list_exchanges_with_funding_rate_history():
    exchanges_with_funding_rate_history = []
    for exchange_id in ccxt.exchanges:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class()
        if exchange.describe().get('has', {}).get('fetchFundingRateHistory', False):
            exchanges_with_funding_rate_history.append(exchange_id)
    return exchanges_with_funding_rate_history

def fetch_funding_rate_history(exchange_id, symbol, since=None, limit=100, params={}):
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class()

        if exchange.has['fetchFundingRateHistory']:
            params['paginate'] = True  # Enable built-in pagination if supported by the exchange
            params['until'] = exchange.milliseconds()  # Set the end time to the current time
            params['paginationDirection'] = 'forward'  # Fetch records from the oldest to the newest
            params['paginationCalls'] = 10  # Number of pagination calls to make. Adjust as needed
            increase_call = ['phemex' 'okx','myokx','bybit']
            if exchange_id in increase_call:
                params['paginationCalls'] = 66
            funding_rate_history = exchange.fetch_funding_rate_history(symbol, since, limit, params)
            return funding_rate_history
        else:
            return f"{exchange_id} does not support fetching funding rate history."
    except Exception as e:
        return f"Error fetching funding rate history: {str(e)}"

def get_directory_and_filename(exchange_id, symbol):
    # Format the base symbol (e.g., BTC_USDT_USDT from  )
    base_symbol = symbol.replace('/', '_').replace(':', '_')
    
    # Create a directory based on the base symbol
    directory = os.path.join(r'C:\Users\agarn\OneDrive\Documents\Data\Funding_Rates', base_symbol)
    
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Create a filename based on the exchange, base symbol, and information type
    base_filename = f"{exchange_id}_{base_symbol}_funding_rate_history"
    filename = os.path.join(directory, f"{base_filename}.csv")
    
    # Ensure the filename is not too long
    max_filename_length = 255
    if len(filename) > max_filename_length:
        filename = filename[:max_filename_length - 4] + ".csv"
    
    return directory, filename

def save_to_csv(data, exchange_id, symbol):
    try:
        directory, filename = get_directory_and_filename(exchange_id, symbol)
        
        # Extract the required keys and their values
        extracted_data = [
            {
                'symbol': entry['symbol'],
                'fundingRate': entry['fundingRate'],
                'timestamp': entry['timestamp'],
                'datetime': entry['datetime']
            }
            for entry in data
        ]
        
        # Define the CSV headers
        headers = ['symbol', 'fundingRate', 'timestamp', 'datetime']

        # Check if the file exists to append data
        file_exists = os.path.isfile(filename)

        # Write data to an in-memory buffer
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=headers)
        if not file_exists:
            writer.writeheader()  # Write headers only if the file does not exist
        writer.writerows(extracted_data)  # Always write the data rows to the buffer
        
        # Get the CSV content from the buffer
        csv_content = buffer.getvalue()
        
        # Write the CSV content to the file
        with open(filename, mode='a', newline='') as file:  # Append mode
            file.write(csv_content)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def main():
    # List of symbols to fetch funding rates for
    symbols = ['BTC/USDT:USDT', 'ETH/USDT:USDT']
    
    # List exchanges that support fetching funding rate history
    exchanges = list_exchanges_with_funding_rate_history()
    print("Exchanges with fetchFundingRateHistory support:", exchanges)

    default_since = 1546300800000  # Default start time in milliseconds

    for exchange_id in exchanges:
        for symbol in symbols:
            directory, filename = get_directory_and_filename(exchange_id, symbol)

            # Determine the 'since' value
            since = default_since
            if os.path.isfile(filename):
                try:
                    with open(filename, mode='r', newline='') as file:
                        reader = csv.DictReader(file)
                        rows = list(reader)
                        if rows:
                            last_timestamp_str = rows[-1]['timestamp']
                            if last_timestamp_str:  # Check if the timestamp is not empty
                                # Check if the timestamp is in scientific notation
                                if re.match(r'^\d+(\.\d+)?[eE][+-]?\d+$', last_timestamp_str):
                                    last_timestamp = int(float(last_timestamp_str))  # Convert from scientific notation to float, then to int
                                else:
                                    last_timestamp = int(last_timestamp_str)  # Convert directly to int
                                since = max(default_since, last_timestamp + 10)
                except Exception as e:
                    print(f"Error reading CSV file: {str(e)}")

            limit = 10000  # Number of records to fetch per request
            if exchange_id == 'bitmex':
                limit = 500
            params = {}  # Additional parameters (optional)

            funding_rate_history = fetch_funding_rate_history(exchange_id, symbol, since, limit, params)
            
            if isinstance(funding_rate_history, list):
                save_to_csv(funding_rate_history, exchange_id, symbol)
            else:
                print(funding_rate_history)

if __name__ == "__main__":
    main()
