import hashlib
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import os
import requests

# InfluxDB connection parameters
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
ORG = "CS 218"
URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
BUCKET = "article_headline"

# List of companies to fetch news for
COMPANIES = ["AAPL", "AMZN", "META", "GOOGL", "NVDA"]

# Get the current time and 48 hours ago (to handle missed data points)
NOW = datetime.utcnow()
FOURTHY8_HOURS_AGO = (NOW - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Initialize InfluxDB client
def init_influxdb_client():
    return InfluxDBClient(url=URL, token=INFLUXDB_TOKEN, org=ORG)

# Fetch data from the news API for a specific company, fetching from the last 24-26 hours
def fetch_news_data(company):
    try:
        params = {
            'q': company,
            'from': FOURTHY8_HOURS_AGO,  # Fetch news from the past 48 hours
            'to': NOW.strftime('%Y-%m-%dT%H:%M:%SZ'),  # Up to the current time
            'sortBy': 'publishedAt',
            'language': 'en',
            'apiKey': os.environ.get("NEWS_API_KEY"),
        }
        response = requests.get("https://newsapi.org/v2/everything", params=params)
        response.raise_for_status()  # Raises HTTPError for bad responses
        news_data = response.json()
        return news_data['articles']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news data for {company}: {e}")
        return None

# Generate a unique hash for company name and headline
def generate_hash(company, headline):
    return hashlib.sha256(f"{company}{headline}".encode()).hexdigest()

# Check if a point with the same hash already exists
def is_duplicate(hash_value, client):
    query = f'''from(bucket: "{BUCKET}")
                |> range(start: 0)
                |> filter(fn: (r) => r._measurement == "news_article" and r.hash == "{hash_value}")
                |> limit(n: 1)'''
    result = client.query_api().query(query)
    return len(result) > 0

# Write news data to InfluxDB
def write_to_influxdb(write_api, company, articles, client):
    try:
        for article in articles:
            # Skip articles where the description is "[Removed]"
            if article['description'] == "[Removed]":
                continue  # Skip to the next article

            # Generate a hash based on company name and headline
            hash_value = generate_hash(company, article['title'])

            # Check if this article (based on hash) already exists in the DB
            if is_duplicate(hash_value, client):
                print(f"Duplicate article for {company}: {article['title']}")
                continue  # Skip the duplicate article

            # Write the non-duplicate article to InfluxDB
            publish_time = datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
            # print(f"Title {article['title']}")
            point = Point("news_article") \
                .tag("source", article['source']['name']) \
                .tag("company", company) \
                .field("title", article['title']) \
                .field("description", article['description']) \
                .field("hash", hash_value) \
                .time(publish_time)
            
            write_api.write(bucket=BUCKET, org=ORG, record=point)
        print(f"Data for {company} successfully written to InfluxDB")
    except Exception as e:
        print(f"Error writing to InfluxDB for {company}: {e}")

# Main function to coordinate fetching and writing for all companies
def main():
    client = init_influxdb_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    for company in COMPANIES:
        articles = fetch_news_data(company)
        
        if articles:
            write_to_influxdb(write_api, company, articles, client)
        else:
            print(f"No articles to write for {company}.")

# Entry point
if __name__ == "__main__":
    main()
