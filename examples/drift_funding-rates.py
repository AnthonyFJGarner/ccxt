#https://drift-labs.github.io/v2-teacher/?python#example-trades-for-date-range
import requests
import csv
from io import StringIO
from datetime import date, timedelta
import pandas as pd

URL_PREFIX = 'https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com/program/dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'

# Method 1: using pandas
def get_trades_for_range_pandas(market_symbol, start_date, end_date):
    all_trades = []
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        url = f"{URL_PREFIX}/market/tradeRecords{market_symbol}/{year}/{year}{month:02}{day:02}"
        #market/${marketSymbol}/fundingRateRecords/${year}/${year}${month}${day}
        #market/marketSymbol/fundingRateRecords/year/yearmonthday

        try:
            df = pd.read_csv(url)
            all_trades.append(df)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {current_date}: {e}")
        except pd.errors.EmptyDataError:
            print(f"No data available for {current_date}")

        current_date += timedelta(days=1)

    if all_trades:
        return pd.concat(all_trades, ignore_index=True)
    else:
        return pd.DataFrame()

# Method 2: using csv reader
def get_trades_for_range_csv(account_key, start_date, end_date):
    all_trades = []
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        url = f"{URL_PREFIX}/user/{account_key}/tradeRecords/{year}/{year}{month:02}{day:02}"
        response = requests.get(url)
        response.raise_for_status()

        csv_data = StringIO(response.text)
        reader = csv.reader(csv_data)
        for row in reader:
            all_trades.append(row)

        current_date += timedelta(days=1)

    return all_trades


# Example usage
account_key = "<Some Account Key>"
market_symbol = "ETH-PERP"
start_date = date(2024, 1, 24)
end_date = date(2024, 1, 26)

trades = get_trades_for_range_pandas(market_symbol, start_date, end_date)