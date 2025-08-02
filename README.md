# Data Harvester

Collects financial data from Reddit and price APIs, uploads to S3 for processing.

## 🎯 Purpose

This module scrapes financial discussions and price data to feed the sentiment analysis pipeline.

## 📊 Data Sources

### Reddit Financial Communities
- **US_STOCKS**: wallstreetbets, investing, stocks, SecurityAnalysis, ValueInvesting
- **CRYPTO**: cryptocurrency, Bitcoin, ethereum, CryptoMarkets  
- **ECONOMICS**: economics, financialindependence

### Price Data
- **Crypto**: BTC, ETH from CoinGecko API
- **Stocks**: ETF prices from Yahoo Finance (with rate limiting)

## 🚀 Usage

### Daily Collection (Recommended)
```bash
python scripts/daily_collection.py
```
Runs both Reddit scraping and price collection in sequence.

### Individual Scripts
```bash
# Reddit data only
python scripts/reddit_scraper.py

# Price data only  
python scripts/price_collector.py
```

## 📁 Output Structure

### S3 Bucket: `automated-trading-data-bucket`
```
raw-data/
├── reddit_financial_YYYYMMDD_HHMMSS.csv
├── price_data_YYYYMMDD_HHMMSS.csv
└── example_source/
    └── YYYYMMDD_HHMMSS.json
```

### CSV Columns

#### Reddit Data
- `id`, `subreddit`, `category`, `title`, `content`
- `url`, `score`, `upvote_ratio`, `num_comments`
- `created_utc`, `author`, `flair`, `type`, `timestamp`

#### Price Data
- `symbol`, `price`, `timestamp`

## ⚙️ Configuration

### Environment Variables (.env)
```bash
# Reddit API (required)
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_secret  
REDDIT_USER_AGENT=TradingBot/1.0

# AWS (required)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1

# S3 (required)
S3_BUCKET_NAME=automated-trading-data-bucket
```

### Reddit API Setup
See `REDDIT_SETUP.md` for detailed instructions on getting Reddit API credentials.

## 📈 Current Performance

- **Reddit**: ~1,300 posts/comments per run
- **Processing Speed**: ~50 posts/second
- **File Size**: ~0.7MB per Reddit collection
- **Price Data**: <0.01MB per collection
- **Categories**: Automatic classification by subreddit

## 🔧 Technical Details

### Rate Limiting
- **Reddit**: Built-in PRAW rate limiting
- **CoinGecko**: No rate limits on free tier
- **Yahoo Finance**: Aggressive rate limiting (may need delays)

### Error Handling
- Skips deleted/removed posts
- Continues on individual subreddit failures
- Logs errors without stopping collection

### Data Quality
- Filters out stickied posts
- Includes post metadata (score, comments, etc.)
- Preserves original timestamps + collection timestamp

## 🎯 Next Steps

### Immediate
- **Automation**: Set up daily cron job or GitHub Actions
- **Monitoring**: Add success/failure notifications
- **Cleanup**: Implement old file retention policy

### Future Enhancements
- **More Assets**: Add individual stock tickers
- **Social Expansion**: Twitter, Discord integration
- **Real-time**: WebSocket connections for live data
- **Quality Filters**: Minimum score/comment thresholds

## 🐛 Known Issues

- **Yahoo Finance**: Rate limiting can cause failures
- **Reddit API**: Occasional timeout on large subreddits
- **S3 Upload**: No retry logic on network failures

## 📊 Data Insights

- **Most Active**: wallstreetbets, cryptocurrency subreddits
- **Peak Times**: US market hours for stock discussions
- **Content Mix**: ~60% posts, ~40% comments
- **Categories**: CRYPTO dominates current data

---

*Part of the automated-trading pipeline*
*Next: ai-workbench processes this data for sentiment analysis*