#!/usr/bin/env python3
"""
Bluesky Financial Data Scraper
Scrapes financial posts from Bluesky public feeds
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
from typing import List, Dict
import re
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv
import time

load_dotenv()

class BlueskyScraper:
    def __init__(self):
        self.s3_uploader = S3Uploader()
        self.base_url = "https://bsky.social/xrpc"
        self.access_token = None
        self.refresh_token = None
        
        # Financial keywords to search for
        self.financial_keywords = [
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'cryptocurrency',
            'stocks', 'stock market', 'trading', 'investing', 'finance',
            'bull market', 'bear market', 'recession', 'inflation',
            'fed', 'federal reserve', 'interest rates', 'sp500', 'nasdaq'
        ]
        
        # Key financial accounts to follow (will need to find their DIDs)
        self.financial_accounts = [
            # Add popular financial accounts as we find them
            # Format: {'handle': 'username.bsky.social', 'did': 'did:plc:...'}
        ]
    
    def authenticate(self):
        """Authenticate with Bluesky using credentials"""
        try:
            username = os.getenv('BLUESKY_USERNAME')  # your.handle.bsky.social
            password = os.getenv('BLUESKY_APP_PASSWORD')  # app password from settings
            
            if not username or not password:
                print("Bluesky credentials not found in .env file")
                return False
            
            # Create session
            url = f"{self.base_url}/com.atproto.server.createSession"
            data = {
                'identifier': username,
                'password': password
            }
            
            response = requests.post(url, json=data)
            if response.status_code == 200:
                session_data = response.json()
                self.access_token = session_data.get('accessJwt')
                self.refresh_token = session_data.get('refreshJwt')
                print("Bluesky authentication successful")
                return True
            else:
                print(f"Bluesky auth failed: {response.text}")
                return False
                
        except Exception as e:
            print(f"Error authenticating with Bluesky: {e}")
            return False
    
    def search_posts(self, query: str, limit: int = 100) -> List[Dict]:
        """Search for posts using authenticated API"""
        posts = []
        
        if not self.access_token:
            print("Not authenticated - cannot search posts")
            return posts
        
        try:
            url = f"{self.base_url}/app.bsky.feed.searchPosts"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            params = {
                'q': query,
                'limit': min(limit, 100),
                'sort': 'latest'
            }
            
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                
                for post in data.get('posts', []):
                    record = post.get('record', {})
                    author = post.get('author', {})
                    
                    post_data = {
                        'id': post.get('uri', '').split('/')[-1],
                        'author_handle': author.get('handle', ''),
                        'author_display_name': author.get('displayName', ''),
                        'text': record.get('text', ''),
                        'created_at': record.get('createdAt', ''),
                        'reply_count': post.get('replyCount', 0),
                        'repost_count': post.get('repostCount', 0),
                        'like_count': post.get('likeCount', 0),
                        'uri': post.get('uri', ''),
                        'query': query,
                        'platform': 'bluesky',
                        'category': 'SOCIAL_MEDIA',
                        'scraped_at': datetime.utcnow(),
                        'timestamp': datetime.utcnow()  # Add timestamp for consistency
                    }
                    posts.append(post_data)
            else:
                print(f"Search failed for '{query}': {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Error searching for '{query}': {e}")
        
        return posts
    
    def get_trending_posts(self, limit: int = 50) -> List[Dict]:
        """Get trending posts from financial feeds"""
        posts = []
        
        try:
            # Get popular posts from the firehose
            url = f"{self.base_url}/app.bsky.feed.getTimeline"
            params = {
                'limit': limit,
                'algorithm': 'reverse-chronological'
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get('feed', []):
                    post = item.get('post', {})
                    record = post.get('record', {})
                    author = post.get('author', {})
                    text = record.get('text', '').lower()
                    
                    # Filter for financial content
                    if any(keyword in text for keyword in self.financial_keywords):
                        post_data = {
                            'id': post.get('uri', '').split('/')[-1],
                            'author_handle': author.get('handle', ''),
                            'author_display_name': author.get('displayName', ''),
                            'text': record.get('text', ''),
                            'created_at': record.get('createdAt', ''),
                            'reply_count': post.get('replyCount', 0),
                            'repost_count': post.get('repostCount', 0),
                            'like_count': post.get('likeCount', 0),
                            'uri': post.get('uri', ''),
                            'query': 'trending_financial',
                            'platform': 'bluesky',
                            'category': 'SOCIAL_MEDIA',
                            'scraped_at': datetime.utcnow(),
                            'timestamp': datetime.utcnow()  # Add timestamp for consistency
                        }
                        posts.append(post_data)
                        
        except Exception as e:
            print(f"Error getting trending posts: {e}")
        
        return posts
    
    def scrape_financial_content(self) -> pd.DataFrame:
        """Scrape financial content from Bluesky using authenticated search"""
        all_posts = []
        
        # Authenticate first
        if not self.authenticate():
            print("Failed to authenticate with Bluesky")
            return pd.DataFrame()
        
        print(f"Searching Bluesky for financial content...")
        
        # Search for financial keywords
        keywords = ['bitcoin', 'crypto', 'stocks', 'trading', 'investing']
        
        for keyword in keywords:
            print(f"Searching for: {keyword}")
            posts = self.search_posts(keyword, 50)
            all_posts.extend(posts)
            
            # Rate limiting - 3000/hour = 50/minute
            time.sleep(2)  # 2 seconds between requests
        
        # Remove duplicates based on URI
        seen_uris = set()
        unique_posts = []
        for post in all_posts:
            if post['uri'] not in seen_uris:
                seen_uris.add(post['uri'])
                unique_posts.append(post)
        
        df = pd.DataFrame(unique_posts)
        print(f"Scraped {len(df)} unique Bluesky financial posts")
        
        return df
    
    def run_scrape_and_upload(self):
        """Main execution: scrape Bluesky and upload to S3"""
        try:
            # Scrape data
            df = self.scrape_financial_content()
            
            if df.empty:
                print("No Bluesky data scraped")
                return
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"bluesky_financial_{timestamp}.csv"
            
            # Upload to S3
            success = self.s3_uploader.upload_dataframe(df, filename)
            
            if success:
                print(f"Successfully uploaded {len(df)} Bluesky posts to S3: {filename}")
            else:
                print("Failed to upload Bluesky data to S3")
                
        except Exception as e:
            print(f"Error in Bluesky scrape and upload: {e}")

if __name__ == "__main__":
    scraper = BlueskyScraper()
    scraper.run_scrape_and_upload()