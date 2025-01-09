import requests
import mysql.connector
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from afinn import Afinn
from uuid import uuid4
from datetime import datetime
from dateutil import parser

# Database Configuration
db_config = {
    'host': 'localhost',
    'database': 'itrust_test',
    'user': 'itrust_test_db',
    'password': 'Itrust@2025'
}

# API Configuration
LIMIT = 500
API_KEY = "U16Gq0PRKGgnTbltSa5423seAWtQNV0T"
API_URL = f"https://financialmodelingprep.com/api/v4/crypto_news?limit={LIMIT}&apikey={API_KEY}"

# Initialize sentiment analyzers
vader_analyzer = SentimentIntensityAnalyzer()
afinn = Afinn()

def fetch_news():
    """Fetch news data from FMP API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to fetch data, status code {response.status_code}")
        return []

def analyze_sentiment(title, description):
    """Analyze sentiment using Vader, TextBlob, and Afinn."""
    combined_text = f"{title} {description}" if description else title

    # Vader Sentiment
    vader_score = vader_analyzer.polarity_scores(combined_text)['compound']

    # TextBlob Sentiment
    textblob_score = TextBlob(combined_text).sentiment.polarity

    # Afinn Sentiment
    afinn_score = afinn.score(combined_text)

    # Calculate overall sentiment score (weighted average)
    sentiment_score = round((vader_score + textblob_score + (afinn_score / 10)) / 3, 2)

    return vader_score, textblob_score, afinn_score, sentiment_score

def save_to_db(news_data):
    """Save news data to the database."""
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    insert_query = (
        "INSERT INTO news (id, title, description, symbol, source, source_url, img, published_at, company, score_tb, "
        "score_vd, score_af, sentiment_score, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    for news in news_data:
        title = news.get('title', '')
        description = news.get('text', '')
        symbol = news.get('symbol', '')
        source = news.get('site', '')
        source_url = news.get('url', '')
        img = news.get('image', '')
        published_at = news.get('publishedDate', None)
        published_at = parser.parse(published_at) if published_at else None

        # Sentiment Analysis
        score_vd, score_tb, score_af, sentiment_score = analyze_sentiment(title, description)

        # Generate UUID
        news_id = str(uuid4())

        # Current timestamp
        timestamp = datetime.now()

        # Insert data into the database
        cursor.execute(insert_query, (
            news_id, title, description, symbol, source, source_url, img, published_at, 'FMP',
            round(score_tb, 2), round(score_vd, 2), round(score_af, 2), round(sentiment_score, 2), timestamp, timestamp
        ))

    connection.commit()
    cursor.close()
    connection.close()

def main():
    """Main function to fetch, analyze, and store news."""
    print("Fetching news data...")
    news_data = fetch_news()

    if news_data:
        print(f"Fetched {len(news_data)} news items. Processing...")
        save_to_db(news_data)
        print("News data has been successfully updated to the database.")
    else:
        print("No news data to process.")

if __name__ == "__main__":
    main()
