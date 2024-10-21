import requests
from influxdb_client import InfluxDBClient, Point
from datetime import datetime
import os
import time
# InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
ORG = "CS 218"
URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
BUCKET_HOURLY = "stock_value_hourly"
BUCKET_DAILY = "stock_value_daily"

# Polygon.io API parameters
API_KEY = os.environ.get("POLYGON_API_KEY")
START_DATE = "2024-10-19"
END_DATE = "2024-10-21"

# List of stock tickers
STOCKS = ["AAPL", "AMZN", "META", "GOOGL", "NVDA"]

# Initialize InfluxDB client
client = InfluxDBClient(url=URL, token=INFLUXDB_TOKEN, org=ORG)
write_api = client.write_api()

def fetch_stock_data(ticker, interval):
    # API URL for fetching stock data based on interval (1 minute, 1 day, etc.)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{interval}/{START_DATE}/{END_DATE}?adjusted=true&sort=asc&limit=50000&apiKey={API_KEY}"
    response = requests.get(url)
    return response.json()

def store_stock_data(data, ticker, bucket_name, interval):
    if not data.get('results'):
        print(f"No stock data available for {ticker}")
        return
    for entry in data['results']:
        # Parse stock data
        volume = int(entry['v'])
        open_price = float(entry['o'])  # Convert to float
        close_price = float(entry['c'])  # Convert to float
        high_price = float(entry['h'])  # Convert to float
        low_price = float(entry['l'])  # Convert to float
        vwap = float(entry['vw'])  # Convert to float
        timestamp = entry['t'] / 1000
        time = datetime.utcfromtimestamp(timestamp)

        # Create a point for InfluxDB
        point = Point("stock_data") \
            .tag("ticker", ticker) \
            .tag("interval", interval) \
            .field("volume", volume) \
            .field("open_price", open_price) \
            .field("close_price", close_price) \
            .field("high_price", high_price) \
            .field("low_price", low_price) \
            .field("vwap", vwap) \
            .time(time)

        # Write the point to the specified bucket
        write_api.write(bucket=bucket_name, org=ORG, record=point)

    print(f"Stock data for {ticker} successfully written to {bucket_name}")

# Loop through all stocks and store hourly and daily data
for stock in STOCKS:
    # Fetch and store hourly data
    hourly_data = fetch_stock_data(stock, 'hour')
    store_stock_data(hourly_data, stock, BUCKET_HOURLY, 'hour')

    # Fetch and store daily data
    #daily_data = fetch_stock_data(stock, 'day')
    #store_stock_data(daily_data, stock, BUCKET_DAILY, 'day')
    time.sleep(12)
