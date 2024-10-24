import os, time
import pandas as pd
from influxdb_client_3 import InfluxDBClient3, Point

token = os.environ.get("INFLUXDB_TOKEN")
org = "CS 218"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, database="article_headline", token=token, org=org)

query = """
SELECT title, time
FROM 'news_article'
WHERE time >= now() - interval '27 hours'
AND company = 'Apple'
ORDER BY time DESC
LIMIT 1
"""

# Execute the query
table = client.query(query=query, language='sql')

# Convert to dataframe
df = table.to_pandas()
print(df)