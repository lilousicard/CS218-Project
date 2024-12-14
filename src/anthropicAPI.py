import anthropic
import os

client = anthropic.Anthropic(api_key=os.environ.get("CLAUDE_API_KEY"))

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

def test_sentiment_scores(client, headlines, examples=None):
    """
    Test sentiment scores for multiple headlines.
    
    Args:
        client: Anthropic API client instance
        headlines (dict): Dictionary of headlines to test
        examples (str, optional): Examples for few-shot learning
    """
    print("Testing Sentiment Scores:")
    print("-" * 50)
    
    for name, headline in headlines.items():
        try:
            score = get_sentiment_score(client, headline, examples)
            print(f"\n{name}:")
            print(f"Headline: {headline}")
            print(f"Score: {score}")
        except Exception as e:
            print(f"\nError processing {name}: {str(e)}")
    
    print("\n" + "-" * 50)

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

headlines = {
    "headline1": "Apple News: Google Tensor leak suggests big efficiency upgrades on Pixel 10 and Pixel 11",
    "headline2": "Google News: Google Tensor leak suggests big efficiency upgrades on Pixel 10 and Pixel 11",
    "headline3": "Apple News: After not playing a snap for the Eagles, Devin White finds a new home"
}


# Run the test
test_sentiment_scores(client, headlines, examples)
print("\nTesting without examples:")
test_sentiment_scores(client, headlines)
