import os
from influxdb_client import InfluxDBClient

# Set your InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = "CS 218"
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_BUCKET = "article_headline"  # Assuming the bucket is article_headline

# Function to fetch the most recent headline for a company
def fetch_most_recent_headline(client, company):
    query_api = client.query_api()
    
    # Flux query to get the most recent headline for the given company
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -30d)  // Fetch records from the last 30 days
      |> filter(fn: (r) => r["_measurement"] == "news_article")
      |> filter(fn: (r) => r["company"] == "{company}")
      |> filter(fn: (r) => r["_field"] == "title" or r["_field"] == "description" or r["_field"] == "source")
      |> last()  // Get the most recent record for each field
    '''
    
    # Execute the query
    tables = query_api.query(query, org=INFLUXDB_ORG)
    
    # Initialize a dictionary to store the most recent headline data
    headline_data = {
        "time": None,
        "title": None,
        "description": None,
        "source": None
    }
    
    # Populate the dictionary with the most recent data for each field
    for table in tables:
        for record in table.records:
            headline_data["time"] = record["_time"]
            if record["_field"] == "title":
                headline_data["title"] = record["_value"]
            if record["_field"] == "description":
                headline_data["description"] = record["_value"]
            if record["_field"] == "source":
                headline_data["source"] = record["_value"]
    
    # Print the most recent headline
    print(f"Most recent headline for {company}:")
    print(f"  Time: {headline_data['time']}")
    print(f"  Title: {headline_data['title'] or 'N/A'}")
    print(f"  Description: {headline_data['description'] or 'N/A'}")
    print(f"  Source: {headline_data['source'] or 'N/A'}")
    print("-" * 40)

# Connect to InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# List of companies you want to query
companies = ['Apple', 'Amazon', 'Meta', 'Google', 'Nvidia']  

# Fetch and display the most recent headline for each company
for company in companies:
    fetch_most_recent_headline(client, company)

# Close the client connection
client.close()
