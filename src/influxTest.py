from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import os

# InfluxDB connection parameters
token = os.environ.get("INFLUXDB_TOKEN")
org = "CS 218"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"
bucket = "stock_value"

# Initialize InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)

# Write API to write data
write_api = client.write_api(write_options=SYNCHRONOUS)

# Write a single point to the database
point = Point("stock_prices") \
    .tag("company", "Apple") \
    .field("price", 150.75) \
    .time(datetime.utcnow())

write_api.write(bucket=bucket, org=org, record=point)
print("Data written to InfluxDB")

# Query the data
query = f'''
from(bucket: "{bucket}")
|> range(start: -1h)
|> filter(fn: (r) => r._measurement == "stock_prices")
|> filter(fn: (r) => r.company == "Apple")
|> filter(fn: (r) => r._field == "price")
'''

query_api = client.query_api()
result = query_api.query(org=org, query=query)

# Process and print the query result
for table in result:
    for record in table.records:
        print(f'Time: {record.get_time()}, Company: {record["company"]}, Price: {record["_value"]}')

# Close the client
client.close()

