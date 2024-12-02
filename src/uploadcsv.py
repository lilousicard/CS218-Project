import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os

# InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
ORG = "CS 218"
URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
BUCKET = "article_test"


# Create InfluxDB client
client = InfluxDBClient(url=URL, token=INFLUXDB_TOKEN)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Load CSV into pandas DataFrame
df = pd.read_csv('/Users/lilou/Code/CS218-Project/influxdata_2024-10-24T17_01_06Z_with_scores.csv')

# Loop through each row in the DataFrame and upload to InfluxDB
for _, row in df.iterrows():
    point = Point(row['_measurement']) \
        .tag("company", row['company']) \
        .tag("source", row['source']) \
        .field("description", row['description']) \
        .field("hash", row['hash']) \
        .field("title", row['title']) \
        .field("score", row['score']) \
        .time(row['time'])  # Convert to nanosecond precision

    # Write the point to InfluxDB
    write_api.write(bucket=BUCKET, org=ORG, record=point)

# Close client connection
client.close()
