import os, time
import pandas as pd
from influxdb_client_3 import InfluxDBClient3, Point

token = os.environ.get("INFLUXDB_TOKEN")
org = "CS 218"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, database="stock_value_hourly", token=token, org=org)

query = """
SELECT *
FROM stock_data sd
JOIN (
    SELECT ticker, MAX(time) AS latest_time
    FROM stock_data
    GROUP BY ticker
) latest ON sd.ticker = latest.ticker AND sd.time = latest.latest_time
ORDER BY sd.ticker
"""

# Execute the query
table = client.query(query=query, language='sql')

# Convert to dataframe
df = table.to_pandas()
print(df)

