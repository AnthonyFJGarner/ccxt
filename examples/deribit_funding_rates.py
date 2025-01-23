import ccxt
import time
import csv
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_funding_rate_history(exchange, instrument, since=None, limit=None, params={'until': None}, retries=3):
    try:
        time_now = exchange.milliseconds()
        month = 30 * 24 * 60 * 60 * 1000
        until = since + month  # Set the end time to one month after the start time
        rates = []
        while since < time_now:
            request = {
                'instrument_name': instrument,
                'start_timestamp': since,
                'end_timestamp': until,
            }
            try:
                funding_rates = exchange.publicGetGetFundingRateHistory(request)
                rates.extend(funding_rates['result'])
            except ccxt.NetworkError as e:
                logging.error(f"Network error: {str(e)}. Retrying...")
                if retries > 0:
                    time.sleep(5)  # Wait before retrying
                    return fetch_funding_rate_history(exchange, instrument, since, limit, params, retries - 1)
                else:
                    logging.error("Max retries reached. Exiting.")
                    return []
            except ccxt.ExchangeError as e:
                logging.error(f"Exchange error: {str(e)}. Exiting.")
                return []
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}. Exiting.")
                return []
            since += month
            until += month
            if until > time_now:
                until = time_now
            # Apply manual rate limiting
            time.sleep(exchange.rateLimit / 1000)
        return rates
    except Exception as e:
        logging.error(f"Error fetching funding rate history: {str(e)}")
        return []

def convert_timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

def group_by_day(data):
    grouped_data = {}
    for entry in data:
        date = convert_timestamp_to_date(entry['timestamp']).date()
        if date not in grouped_data:
            grouped_data[date] = []
        grouped_data[date].append(entry)
    return grouped_data

def select_entries(grouped_data):
    selected_entries = []
    for date, entries in grouped_data.items():
        if len(entries) >= 24:
            selected_entries.append(entries[7])  # 8th entry (index 7)
            selected_entries.append(entries[15]) # 16th entry (index 15)
            selected_entries.append(entries[23]) # 24th entry (index 23)
    return selected_entries

def save_to_csv(data, filename):
    if not data:
        return
    # Add 'date' to the headers and make it the first column
    keys = ['date'] + list(data[0].keys())
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        for entry in data:
            # Add the human-readable date in UTC to each entry
            entry['date'] = convert_timestamp_to_date(entry['timestamp']).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Reorder the dictionary to make 'date' the first key
            ordered_entry = {key: entry[key] for key in keys}
            dict_writer.writerow(ordered_entry)

def main():
    symbol = 'ETH/USD:ETH' 
    exchange_id = 'deribit'
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({
        'enableRateLimit': True,  # Enable rate limiting
    })
    exchange.load_markets()
    instrument = exchange.markets[symbol]['id']
    since = exchange.parse8601('2019-01-01T00:00:00') - 1
    limit = 1000

    # Fetch funding rate history
    funding_rate_history = fetch_funding_rate_history(exchange, instrument, since, limit, params={'until': None})

    # Group by day and select specific entries
    grouped_data = group_by_day(funding_rate_history)
    selected_entries = select_entries(grouped_data)

    # Save to CSV
    save_to_csv(selected_entries, r'C:\Users\agarn\OneDrive\Documents\Data\Funding_Rates\selected_funding_rates.csv')

if __name__ == "__main__":
    main()
    print("Done")