# Reddit API Setup

To scrape Reddit data, you need to create a Reddit application and get API credentials.

## Steps:

1. **Go to Reddit Apps**: https://www.reddit.com/prefs/apps
2. **Create New App**:
   - Name: `Trading Data Scraper`
   - Type: `script`
   - Description: `Financial sentiment analysis`
   - Redirect URI: `http://localhost:8080` (required but not used)
3. **Get Credentials**:
   - **Client ID**: The string under your app name (14 characters)
   - **Client Secret**: The "secret" field (27 characters)

## Add to .env file:

```bash
REDDIT_CLIENT_ID=your_14_char_client_id
REDDIT_CLIENT_SECRET=your_27_char_client_secret
REDDIT_USER_AGENT=TradingBot/1.0
```

## Test the scraper:

```bash
pip install -r requirements.txt
python3 scripts/reddit_scraper.py
```

## Subreddits Covered:

**Stocks:**
- r/wallstreetbets - Meme stocks, high sentiment
- r/investing - Serious discussion
- r/stocks - General stock talk
- r/SecurityAnalysis - Fundamental analysis
- r/ValueInvesting - Value investing

**Crypto:**
- r/cryptocurrency - General crypto discussion
- r/Bitcoin - Bitcoin specific
- r/ethereum - Ethereum specific
- r/CryptoMarkets - Trading focused

## Rate Limits:

- Reddit API: 60 requests per minute
- Scraper automatically handles rate limiting
- Scrapes ~500-1000 posts/comments per run