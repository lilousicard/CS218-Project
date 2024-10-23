import os, time
import pandas as pd
from influxdb_client_3 import InfluxDBClient3, Point

token = os.environ.get("INFLUXDB_TOKEN")
org = "CS 218"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, database="article_headline", token=token, org=org)

query = """
SELECT na.title, na.time, na.company
FROM news_article na
JOIN (
    SELECT company, MAX(time) AS latest_time
    FROM news_article
    GROUP BY company
) latest ON na.company = latest.company AND na.time = latest.latest_time
ORDER BY na.company
"""

# Execute the query
table = client.query(query=query, language='sql')

# Convert to dataframe
df = table.to_pandas()
print(df)
