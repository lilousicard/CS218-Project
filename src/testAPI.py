import requests
from influxdb_client import InfluxDBClient, Point
import os

# InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
ORG = "CS 218"
URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
BUCKET_HOURLY = "stock_value_hourly"
API_KEY = os.environ.get("POLYGON_API_KEY")
TICKER = "AAPL"
START_DATE = "2023-01-09"
END_DATE = "2023-01-09"

# Initialize InfluxDB client
client = InfluxDBClient(url=URL, token=INFLUXDB_TOKEN, org=ORG)
write_api = client.write_api()

def fetch_stock_data(ticker, interval):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{interval}/{START_DATE}/{END_DATE}?apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        
        # Print the status code and response text for debugging
        print(f"API Response Status Code: {response.status_code}")
        print(f"API Response Content: {response.content.decode('utf-8')}")

        # Convert response to JSON
        data = response.json()

        # Debug: Check if 'results' key exists
        if 'results' in data:
            return data['results']
        else:
            # Handle missing 'results' key
            print("Error: 'results' key not found in the response.")
            print(f"Full Response: {data}")
            return None
    except Exception as e:
        # Print any exceptions that occur during the API call or JSON parsing
        print(f"Error fetching stock data: {e}")
        return None

# Example call to fetch and debug the stock data
data = fetch_stock_data(TICKER, 'hour')

if data:
    for entry in data:
        print(entry)  # Process the individual stock data points
