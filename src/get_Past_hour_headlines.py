import os
from influxdb_client import InfluxDBClient

# Set your InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = "CS 218"
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_BUCKET = "article_headline"  # Assuming the bucket is article_headline

def fetch_recent_headlines(client, company):
    query_api = client.query_api()
    
    # Flux query to get headlines from the past 1 day for the given company
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -25h)  // Fetch records from the last 1 day
      |> filter(fn: (r) => r["_measurement"] == "news_article")  
      |> filter(fn: (r) => r["company"] == "{company}")
    '''
    
    # Execute the query
    tables = query_api.query(query, org=INFLUXDB_ORG)
    
    # Dictionary to store all headline data grouped by timestamp and source
    grouped_headlines = {}
    
    # Process each record directly
    for table in tables:
        for record in table.records:
            timestamp = record["_time"]
            source = record["source"]  # 'source' is a tag
            
            # Use timestamp + source as a unique key for grouping
            key = (timestamp, source)
            
            if key not in grouped_headlines:
                # Initialize a new entry for this timestamp + source
                grouped_headlines[key] = {
                    "time": timestamp,
                    "title": None,
                    "description": None,
                    "source": source
                }
            
            # Populate the title or description depending on the _field
            if record["_field"] == "title":
                grouped_headlines[key]["title"] = record["_value"]
            elif record["_field"] == "description":
                grouped_headlines[key]["description"] = record["_value"]
    
    # Convert dictionary to a list and sort by timestamp
    sorted_headlines = sorted(grouped_headlines.values(), key=lambda x: x["time"])
    
    # Pretty print the headlines with better formatting
    print(f"\nRecent headlines for {company} (within the last day):\n")
    for headline in sorted_headlines:
        print(f"Time:        {headline['time']}")
        print(f"Title:       {headline['title'] or 'N/A'}")
        print(f"Description: {headline['description'] or 'N/A'}")
        print(f"Source:      {headline['source'] or 'N/A'}")
        print("-" * 60)







# Connect to InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# List of companies you want to query
companies = ['Apple']  

# Fetch and display the most recent headline for each company
for company in companies:
    fetch_recent_headlines(client, company)

# Close the client connection
client.close()