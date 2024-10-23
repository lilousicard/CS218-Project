from openai import OpenAI

# Initialize the OpenAI client
client = OpenAI()

# Function to get sentiment analysis
def get_sentiment_gpt4o(article_text):
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"On a scale from -10 (most negative), 0 (neutral), to 10 (most positive), rate the sentiment of this news article: '{article_text}'. Give me only the value of your score"}
        ]
    )
    return completion.choices[0].message.content

# Example news article
article = "Apple's latest earnings report shows a significant decline in sales."

# Get sentiment analysis for the article
sentiment = get_sentiment_gpt4o(article)
print(f"Sentiment: {sentiment}")
