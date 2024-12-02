import pandas as pd
import time
from datetime import datetime
import anthropic
import os
import threading
import logging

anthropic_client = anthropic.Anthropic(api_key=os.environ.get("CLAUDE_API_KEY"))


file_path = "/Users/lilou/Code/CS218-Project/influxdata_2024-10-24T17_01_06Z.csv"


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

def process_csv_with_sentiment(file_path, rate_limiter, anthropic_client, examples):
    """
    Process CSV file and add sentiment scores
    """
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Initialize sentiment scores column
    df['score'] = None
    
    # Process each headline
    for idx, row in df.iterrows():
        # Construct headline from company name, title and description
        headline = f"{row['company']} news: {row['title']} {row['description']}"
        print(headline)
        # Get sentiment score with rate limiting
        try:
            rate_limiter.wait_if_needed()
            score = get_sentiment_score(anthropic_client, headline, examples)
            df.at[idx, 'score'] = score
            print(f"Processed row {idx + 1}/{len(df)}: Score = {score}")
        except Exception as e:
            print(f"Error processing row {idx + 1}: {str(e)}")
    
    # Save processed CSV
    output_file = file_path.replace('.csv', '_with_scores.csv')
    df.to_csv(output_file, index=False)
    print(f"\nSaved processed file to: {output_file}")
    
    return df

def get_sentiment_score(client, headline, examples=None):
    """
    Get sentiment score for a news headline using Claude API.
    
    Args:
        client: Anthropic API client instance
        headline (str): The news headline to analyze
        examples (str, optional): Examples to include in the prompt for few-shot learning
        
    Returns:
        dict: Complete response from the API
    """
    # Construct the messages list
    message_content = []
    
    # Add examples if provided
    if examples:
        message_content.append({
            "type": "text",
            "text": examples
        })
    
    # Add the actual headline
    message_content.append({
        "type": "text",
        "text": headline
    })
    
    # Create the API request
    response = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=1000,
        temperature=0,
        system="You are an assistant that gets news headlines about a company. You need to judge the overall sentiment of the article on a scale of -10 (most negative), 0 (neutral), to 10 (most positive). If the article headline seems like noise, give it a score of 101 so that I can remove it from the ML dataset. Give me only the score.",
        messages=[
            {
                "role": "user",
                "content": message_content
            }
        ]
    )
    
    # Extract the score from the response
    score = int(response.content[0].text)
    return score


def main():

    rate_limiter = RateLimiter(max_requests=50, time_window=60)
    
    # Example template for sentiment analysis
    examples = """<examples>
    <example>
    <headline>
    New Apple MacBook Pro Laptops Are Days Away From Launch - Forbes
    </headline>
    <ideal_output>
    9
    </ideal_output>
    </example>
    </examples>
    """
    
    
    # Process CSV and add sentiment scores
    df = process_csv_with_sentiment(file_path, rate_limiter, anthropic_client, examples)

if __name__ == "__main__":
    main()