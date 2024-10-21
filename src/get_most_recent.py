import os
from influxdb_client import InfluxDBClient

# Set your InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = "CS 218"
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_BUCKET = "stock_value_hourly"

# Function to fetch the most recent transaction for a stock
def fetch_most_recent_transaction(client, ticker):
    query_api = client.query_api()
    
    # Flux query to get the most recent data point for the given ticker
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -30d)  
      |> filter(fn: (r) => r["_measurement"] == "stock_data")  // Adjust this if your measurement is different
      |> filter(fn: (r) => r["ticker"] == "{ticker}")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1)  // Get the most recent record
    '''
    
    # Execute the query
    tables = query_api.query(query, org=INFLUXDB_ORG)
    
    # Initialize a dictionary to store field values for the most recent transaction
    transaction_data = {
        "time": None,
        "close_price": None,
        "high_price": None,
        "low_price": None,
        "open_price": None,
        "volume": None,
        "vwap": None
    }
    
    # Populate the dictionary with the most recent data for each field
    for table in tables:
        for record in table.records:
            transaction_data["time"] = record["_time"]
            if record["_field"] in transaction_data:
                transaction_data[record["_field"]] = record["_value"]
    
    # Print the most recent transaction
    print(f"Most recent transaction for {ticker}:")
    print(f"  Time: {transaction_data['time']}")
    print(f"  Close Price: {transaction_data['close_price'] or 'N/A'}")
    print(f"  High Price: {transaction_data['high_price'] or 'N/A'}")
    print(f"  Low Price: {transaction_data['low_price'] or 'N/A'}")
    print(f"  Open Price: {transaction_data['open_price'] or 'N/A'}")
    print(f"  Volume: {transaction_data['volume'] or 'N/A'}")
    print(f"  VWAP: {transaction_data['vwap'] or 'N/A'}")
    print("-" * 40)

# Connect to InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# List of stock tickers you want to query
tickers = ['AAPL', 'AMZN', 'GOOGL', 'META', 'NVDA']  

# Fetch and display the most recent transaction for each stock
for ticker in tickers:
    fetch_most_recent_transaction(client, ticker)

# Close the client connection
client.close()

