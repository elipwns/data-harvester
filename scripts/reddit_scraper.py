#!/usr/bin/env python3
"""
Reddit Financial Data Scraper
Scrapes posts and comments from financial subreddits for sentiment analysis
"""

import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
from typing import List, Dict
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv

load_dotenv()

class RedditScraper:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'TradingBot/1.0')
        )
        self.s3_uploader = S3Uploader()
        
        # Financial keywords for filtering news posts
        self.financial_keywords = {
            'companies': ['tesla', 'apple', 'microsoft', 'google', 'amazon', 'meta', 'nvidia', 'bitcoin', 'ethereum', 'bullish'],
            'tickers': ['tsla', 'aapl', 'msft', 'googl', 'amzn', 'meta', 'nvda', 'btc', 'eth', 'blsh'],
            'financial_terms': ['stock', 'shares', 'earnings', 'revenue', 'profit', 'market', 'trading', 'investment', 'crypto', 'ipo', 'bullish']
        }
        
        # Financial subreddits organized by market category
        self.subreddit_categories = {
            'US_STOCKS': [
                'wallstreetbets',    # Retail/meme sentiment
                'investing',         # Conservative investing
                'stocks',           # General stock discussion
                'SecurityAnalysis', # Fundamental analysis
                'ValueInvesting',    # Value investing approach
                'deepfuckingvalue',  # DFV/GME community
                'shortsqueeze',      # Short squeeze plays
                'smallstreetbets',   # Smaller account trading
                'thetagang',         # Options selling strategies
                'wsbafterhours',     # After-hours WSB discussion
                'wallstreetbetselite', # WSB alternative
                'options',           # Options trading
                'RobinHood'          # Robinhood app users
            ],
            'IPOS': [
                'SecurityAnalysis',  # IPO analysis
                'investing',         # IPO discussions
                'stocks',           # IPO coverage
                'RobinHood'         # IPO notifications
            ],
            'CRYPTO': [
                'cryptocurrency',   # General crypto discussion
                'Bitcoin',          # Bitcoin specific
                'ethereum',         # Ethereum specific
                'CryptoMarkets'     # Crypto trading focused
            ],
            'ECONOMICS': [
                'economics',        # Economic policy/macro
                'financialindependence',  # Long-term wealth sentiment
                'atrioc'           # Streamer community - politics/stocks/current events
            ],
            'NEWS': [
                'news',            # General news with market impact
                'technology',      # Tech company developments
                'business',        # Business news and analysis
                'worldnews',       # Global events affecting markets
                'politics'         # Policy changes affecting sectors
            ]
        }
        
        # Flatten for backward compatibility
        self.subreddits = []
        for category, subs in self.subreddit_categories.items():
            self.subreddits.extend(subs)
    
    def is_financially_relevant(self, title, content):
        """Check if a post contains financial keywords"""
        text = f"{title} {content}".lower()
        
        # Check for any financial keywords
        for keyword_list in self.financial_keywords.values():
            for keyword in keyword_list:
                if keyword in text:
                    return True
        return False
    
    def scrape_subreddit_posts(self, subreddit_name: str, limit: int = 100) -> List[Dict]:
        """Scrape hot posts from a subreddit"""
        posts = []
        
        # Find which category this subreddit belongs to
        category = 'OTHER'
        for cat, subs in self.subreddit_categories.items():
            if subreddit_name in subs:
                category = cat
                break
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            for post in subreddit.hot(limit=limit):
                # Skip stickied posts
                if post.stickied:
                    continue
                
                # Filter news posts for financial relevance
                if category == 'NEWS':
                    if not self.is_financially_relevant(post.title, post.selftext):
                        continue
                
                post_data = {
                    'id': post.id,
                    'subreddit': subreddit_name,
                    'category': category,  # Add market category
                    'title': post.title,
                    'content': post.selftext if post.selftext else '',
                    'url': post.url,
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'author': str(post.author) if post.author else '[deleted]',
                    'flair': post.link_flair_text,
                    'type': 'post',
                    'timestamp': datetime.utcnow()
                }
                posts.append(post_data)
                
        except Exception as e:
            print(f"Error scraping r/{subreddit_name}: {e}")
        
        return posts
    
    def scrape_post_comments(self, post_id: str, limit: int = 50) -> List[Dict]:
        """Scrape top comments from a specific post"""
        comments = []
        
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)  # Remove "more comments"
            
            for comment in submission.comments[:limit]:
                if hasattr(comment, 'body') and comment.body != '[deleted]':
                    # Find category for this subreddit
                    category = 'OTHER'
                    subreddit_name = submission.subreddit.display_name
                    for cat, subs in self.subreddit_categories.items():
                        if subreddit_name in subs:
                            category = cat
                            break
                    
                    comment_data = {
                        'id': comment.id,
                        'post_id': post_id,
                        'subreddit': subreddit_name,
                        'category': category,  # Add market category
                        'content': comment.body,
                        'score': comment.score,
                        'created_utc': datetime.fromtimestamp(comment.created_utc),
                        'author': str(comment.author) if comment.author else '[deleted]',
                        'type': 'comment',
                        'timestamp': datetime.utcnow()
                    }
                    comments.append(comment_data)
                    
        except Exception as e:
            print(f"Error scraping comments for post {post_id}: {e}")
        
        return comments
    
    def scrape_all_subreddits(self, posts_per_sub: int = 50) -> pd.DataFrame:
        """Scrape posts from all financial subreddits"""
        all_data = []
        
        print(f"Scraping {len(self.subreddits)} financial subreddits...")
        
        for subreddit_name in self.subreddits:
            print(f"Scraping r/{subreddit_name}...")
            posts = self.scrape_subreddit_posts(subreddit_name, posts_per_sub)
            all_data.extend(posts)
            
            # Get comments from top posts
            top_posts = sorted(posts, key=lambda x: x['score'], reverse=True)[:5]
            for post in top_posts:
                comments = self.scrape_post_comments(post['id'], 20)
                all_data.extend(comments)
        
        df = pd.DataFrame(all_data)
        print(f"Scraped {len(df)} total items ({len(df[df['type']=='post'])} posts, {len(df[df['type']=='comment'])} comments)")
        
        return df
    
    def run_scrape_and_upload(self):
        """Main execution: scrape data and upload to S3"""
        try:
            # Scrape data
            df = self.scrape_all_subreddits()
            
            if df.empty:
                print("No data scraped")
                return
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"reddit_financial_{timestamp}.csv"
            
            # Upload to S3
            success = self.s3_uploader.upload_dataframe(df, filename)
            
            if success:
                print(f"Successfully uploaded {len(df)} records to S3: {filename}")
            else:
                print("Failed to upload to S3")
                
        except Exception as e:
            print(f"Error in scrape and upload: {e}")

if __name__ == "__main__":
    scraper = RedditScraper()
    scraper.run_scrape_and_upload()