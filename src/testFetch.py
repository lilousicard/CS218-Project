import requests
import os
from datetime import datetime

# Fetch the API key from environment variables
API_KEY = os.environ.get("POLYGON_API_KEY")

# Check if API_KEY is available
if not API_KEY:
    raise ValueError("API key not found. Please set the POLYGON_API_KEY environment variable.")

# Base URL for the API call
base_url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/hour/2024-09-19/2024-10-19?adjusted=true&sort=asc&limit=50000&apiKey={API_KEY}"

# Function to fetch and print timestamps
def fetch_data(url):
    while url:
        # Fetch the data
        response = requests.get(url)

        # Check if request was successful
        if response.status_code == 200:
            data = response.json()

            # Check if the results key exists
            if 'results' in data:
                # Loop through each result and print the timestamp in a readable format
                for result in data['results']:
                    # Convert Unix timestamp (milliseconds) to date and hour
                    timestamp = result['t'] / 1000  # Convert milliseconds to seconds
                    readable_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    print(readable_time)

            # Check if there's a next page
            url = data.get('next_url', None)
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

# Start fetching data from the base URL
fetch_data(base_url)

