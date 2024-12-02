import influxdb_client_3
import anthropic
from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime
import time
import os
import threading
import logging

class RateLimiter:
    def __init__(self, max_requests, time_window):
        """
        Initialize rate limiter
        
        Args:
            max_requests (int): Maximum number of requests allowed
            time_window (int): Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window  # in seconds
        self.requests = []
        self.lock = threading.Lock()
        
    def can_make_request(self):
        """Check if a request can be made under current rate limits"""
        current_time = time.time()
        
        # Remove requests older than the time window
        with self.lock:
            self.requests = [req_time for req_time in self.requests 
                           if current_time - req_time < self.time_window]
            
            if len(self.requests) < self.max_requests:
                self.requests.append(current_time)
                return True
                
            return False
    
    def wait_if_needed(self):
        """Wait until a request can be made"""
        while not self.can_make_request():
            time.sleep(1)  # Wait for 1 second before checking again

class NewsAnalyzer:
    def __init__(self, client, rate_limiter, anthropic_client, examples=None):
        """
        Initialize NewsAnalyzer with necessary clients and configuration
        """
        self.db_client = client
        self.rate_limiter = rate_limiter
        self.anthropic_client = anthropic_client
        self.examples = examples

    def add_score_column(self, database, table):
        """
        Add score column to the table if it doesn't exist
        """
        try:
            # SQL query to add the score column
            alter_query = f"""
                ALTER TABLE "{database}"."{table}"
                ADD COLUMN score DOUBLE
            """
            
            self.db_client.query(query=alter_query)
            print("Score column added successfully")
            return True
        except Exception as e:
            print(f"Error adding score column: {str(e)}")
            return False

    def fetch_headlines(self, database, table, start_time=None, end_time=None):
        """
        Fetch headlines from InfluxDB using SQL
        """
        # Base query - since score column is new, we don't filter by it
        query = f"""
            SELECT time, title
            FROM "{table}"
        """
        
        # Add time constraints if provided
        if start_time:
            query += f" WHERE time >= '{start_time}'"
            if end_time:
                query += f" AND time <= '{end_time}'"
        elif end_time:
            query += f" WHERE time <= '{end_time}'"
            
        try:
            # Execute query
            results = self.db_client.query(query=query)
            return results
        except Exception as e:
            print(f"Error fetching headlines: {str(e)}")
            return None

    def update_score(self, database, table, timestamp, headline, score):
        """
        Update the score for a specific headline
        """
        try:
            # Update query with parameters
            update_query = f"""
                INSERT INTO "{database}"."{table}" (
                    time,
                    headline,
                    score
                )
                VALUES (
                    $timestamp,
                    $headline,
                    {score}
                )
            """
            
            # Execute update with parameters
            params = {
                "timestamp": timestamp,
                "headline": headline
            }
            
            self.db_client.query(query=update_query, params=params)
            return True
        except Exception as e:
            print(f"Error updating score: {str(e)}")
            return False

    def process_headlines(self, database, table, batch_size=10):
        """
        Process headlines and add sentiment scores
        """
        # First, add the score column
        if not self.add_score_column(database, table):
            print("Failed to add score column. Existing scores will be updated if the column already exists.")
        
        # Fetch headlines
        results = self.fetch_headlines(database, table)
        if not results:
            print("No headlines to process")
            return
        
        processed_count = 0
        error_count = 0
        
        for record in results:
            try:
                # Extract headline and timestamp
                headline = record.get('headline')
                timestamp = record.get('_time')
                
                if not headline:
                    continue
                
                # Get sentiment score with rate limiting
                score = get_sentiment_score_with_rate_limit(
                    self.anthropic_client, 
                    headline, 
                    self.rate_limiter, 
                    self.examples
                )
                
                if score is not None:
                    # Update the score in the database
                    success = self.update_score(database, table, timestamp, headline, score)
                    
                    if success:
                        processed_count += 1
                        print(f"Processed {processed_count} headlines")
                    else:
                        error_count += 1
                
                # Optional: Add delay between batches
                if processed_count % batch_size == 0:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error processing headline: {str(e)}")
                error_count += 1
                continue
        
        print(f"\nProcessing complete:")
        print(f"Successfully processed: {processed_count} headlines")
        print(f"Errors: {error_count}")

# Example usage:
def main():
    # InfluxDB Cloud configuration
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "CS 218"
    url = "https://us-east-1-1.aws.cloud2.influxdata.com"
    database = "article_headline"
    table = "news_article"

    # Example usage:
    examples = """<examples>
    <example>
    <headline>
    Apple News: New Apple MacBook Pro Laptops Are Days Away From Launch - Forbes
    </headline>
    <ideal_output>
    9
    </ideal_output>
    </example>
    </examples>

    """
    
    # Initialize InfluxDB client
    client = InfluxDBClient3(
        host=url,
        database="article_headline",
        token=token,
        org=org
    )

    anthropic_client = anthropic.Anthropic(api_key=os.environ.get("CLAUDE_API_KEY"))
    
    # Initialize rate limiter (50 requests per minute)
    rate_limiter = RateLimiter(max_requests=50, time_window=60)
    
    # Initialize analyzer
    analyzer = NewsAnalyzer(
        client=client,
        rate_limiter=rate_limiter,
        anthropic_client=anthropic_client,
        examples=examples
    )
    
    # Process headlines
    analyzer.process_headlines(database, table)
    
    # Close the client
    client.close()

if __name__ == "__main__":
    main()