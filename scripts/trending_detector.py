#!/usr/bin/env python3
"""
Trending Opportunities Detector
Multi-signal detection system for emerging investment opportunities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv
import boto3
from io import StringIO

load_dotenv()

class TrendingDetector:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_uploader = S3Uploader()
        
        # Signal weights for composite scoring
        self.signals = {
            'reddit_mentions': 0.4,      # 40% weight - primary signal
            'volume_spike': 0.3,         # 30% weight - market validation
            'price_movement': 0.2,       # 20% weight - momentum
            'sentiment_shift': 0.1       # 10% weight - sentiment change
        }
        
        # Common stock/crypto symbols to track
        self.tracked_symbols = [
            # Meme stocks
            'GME', 'AMC', 'BBBY', 'NOK', 'BB', 'PLTR', 'WISH', 'CLOV',
            # Crypto
            'BTC', 'ETH', 'DOGE', 'SHIB', 'ADA', 'SOL', 'MATIC', 'AVAX',
            # Popular stocks
            'TSLA', 'AAPL', 'NVDA', 'MSFT', 'GOOGL', 'META', 'AMZN', 'NFLX',
            # Recent IPOs/SPACs
            'RIVN', 'LCID', 'HOOD', 'COIN', 'RBLX', 'SNOW', 'ABNB', 'BLSH'
        ]
    
    def load_recent_reddit_data(self, days=7):
        """Load recent Reddit data from S3"""
        try:
            objects = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="raw-data/reddit_financial_"
            )
            
            if not objects.get('Contents'):
                return pd.DataFrame()
            
            # Get recent files (last 7 days)
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_files = [
                obj for obj in objects['Contents'] 
                if obj['LastModified'].replace(tzinfo=None) >= cutoff_date
            ]
            
            all_data = []
            for obj in recent_files:
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=obj['Key']
                )
                csv_content = response['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_content))
                all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                return combined_df
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error loading Reddit data: {e}")
            return pd.DataFrame()
    
    def extract_symbols_from_text(self, text):
        """Extract stock/crypto symbols from text using regex"""
        if not isinstance(text, str):
            return []
        
        # Look for $SYMBOL or SYMBOL patterns
        patterns = [
            r'\$([A-Z]{2,5})\b',  # $GME, $BTC
            r'\b([A-Z]{2,5})\b'   # GME, BTC (standalone)
        ]
        
        symbols = []
        for pattern in patterns:
            matches = re.findall(pattern, text.upper())
            symbols.extend(matches)
        
        # Filter to tracked symbols only
        return [s for s in symbols if s in self.tracked_symbols]
    
    def calculate_mention_baseline(self, df, symbol, days=30):
        """Calculate baseline mention frequency for a symbol"""
        if df.empty:
            return 0
        
        # Look for symbol mentions in title and content
        df['mentions_symbol'] = df.apply(
            lambda row: symbol in self.extract_symbols_from_text(
                f"{row.get('title', '')} {row.get('content', '')}"
            ), axis=1
        )
        
        # Calculate daily mention counts
        df['date'] = df['timestamp'].dt.date
        daily_mentions = df[df['mentions_symbol']].groupby('date').size()
        
        if len(daily_mentions) == 0:
            return 0
        
        # Return average daily mentions over the period
        return daily_mentions.mean()
    
    def detect_mention_spikes(self, df):
        """Detect unusual mention spikes for tracked symbols"""
        if df.empty:
            return []
        
        trending_items = []
        
        # Get recent data (last 24 hours) vs baseline (last 30 days)
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_df = df[df['timestamp'] >= recent_cutoff].copy()
        
        for symbol in self.tracked_symbols:
            # Calculate baseline (30-day average)
            baseline = self.calculate_mention_baseline(df, symbol, days=30)
            
            if baseline == 0:
                continue  # Skip symbols with no historical mentions
            
            # Calculate recent mentions (last 24 hours)
            recent_mentions = 0
            for _, row in recent_df.iterrows():
                text = f"{row.get('title', '')} {row.get('content', '')}"
                if symbol in self.extract_symbols_from_text(text):
                    recent_mentions += 1
            
            # Calculate spike ratio
            spike_ratio = recent_mentions / max(baseline, 0.1)  # Avoid division by zero
            
            if spike_ratio >= 3.0:  # 3x normal mentions = trending
                trending_items.append({
                    'symbol': symbol,
                    'recent_mentions': recent_mentions,
                    'baseline_mentions': baseline,
                    'spike_ratio': spike_ratio,
                    'reddit_score': min(spike_ratio / 10, 1.0),  # Normalize to 0-1
                    'detected_at': datetime.now()
                })
        
        return trending_items
    
    def get_volume_spike_score(self, symbol):
        """Get volume spike score (placeholder - would integrate with price data)"""
        # This would integrate with your existing price data
        # For now, return random score for demonstration
        import random
        return random.uniform(0, 1)
    
    def get_price_movement_score(self, symbol):
        """Get price movement score (placeholder)"""
        import random
        return random.uniform(0, 1)
    
    def get_sentiment_shift_score(self, symbol, df):
        """Calculate sentiment shift score for a symbol"""
        if df.empty or 'sentiment_label' not in df.columns:
            return 0
        
        # Filter posts mentioning this symbol
        symbol_posts = []
        for _, row in df.iterrows():
            text = f"{row.get('title', '')} {row.get('content', '')}"
            if symbol in self.extract_symbols_from_text(text):
                symbol_posts.append(row)
        
        if len(symbol_posts) < 5:  # Need minimum posts for sentiment analysis
            return 0
        
        symbol_df = pd.DataFrame(symbol_posts)
        
        # Calculate recent vs historical sentiment
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_sentiment = symbol_df[symbol_df['timestamp'] >= recent_cutoff]
        historical_sentiment = symbol_df[symbol_df['timestamp'] < recent_cutoff]
        
        if recent_sentiment.empty or historical_sentiment.empty:
            return 0
        
        # Calculate bullish percentage
        def calc_bullish_pct(sentiment_df):
            bullish = sentiment_df['sentiment_label'].isin(['4 stars', '5 stars']).mean()
            return bullish
        
        recent_bullish = calc_bullish_pct(recent_sentiment)
        historical_bullish = calc_bullish_pct(historical_sentiment)
        
        # Return sentiment shift (positive = more bullish)
        return recent_bullish - historical_bullish
    
    def calculate_composite_score(self, item, df):
        """Calculate composite trending score using all signals"""
        symbol = item['symbol']
        
        # Get individual signal scores
        reddit_score = item['reddit_score']
        volume_score = self.get_volume_spike_score(symbol)
        price_score = self.get_price_movement_score(symbol)
        sentiment_score = max(0, self.get_sentiment_shift_score(symbol, df))  # Only positive sentiment shifts
        
        # Calculate weighted composite score
        composite = (
            reddit_score * self.signals['reddit_mentions'] +
            volume_score * self.signals['volume_spike'] +
            price_score * self.signals['price_movement'] +
            sentiment_score * self.signals['sentiment_shift']
        )
        
        return {
            'composite_score': composite,
            'reddit_score': reddit_score,
            'volume_score': volume_score,
            'price_score': price_score,
            'sentiment_score': sentiment_score
        }
    
    def get_alert_level(self, score):
        """Convert composite score to alert level"""
        if score >= 0.8:
            return 'EXTREME'
        elif score >= 0.6:
            return 'HIGH'
        elif score >= 0.4:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_risk_warning(self, alert_level):
        """Get risk warning for alert level"""
        warnings = {
            'EXTREME': 'EXTREME RISK - 90%+ failure rate, most likely pump & dump',
            'HIGH': 'HIGH RISK - Volatile, significant risk of loss',
            'MEDIUM': 'MEDIUM RISK - Elevated risk, do your research',
            'LOW': 'LOW RISK - Minor elevation above normal'
        }
        return warnings.get(alert_level, '')
    
    def detect_trending_opportunities(self):
        """Main detection function - find all trending opportunities"""
        print("Detecting trending opportunities...")
        
        # Load recent Reddit data
        df = self.load_recent_reddit_data(days=7)
        if df.empty:
            print("No Reddit data available for analysis")
            return []
        
        print(f"Analyzing {len(df)} Reddit posts/comments from last 7 days...")
        
        # Detect mention spikes
        mention_spikes = self.detect_mention_spikes(df)
        print(f"Found {len(mention_spikes)} symbols with mention spikes")
        
        # Calculate composite scores for each trending item
        trending_opportunities = []
        for item in mention_spikes:
            scores = self.calculate_composite_score(item, df)
            
            opportunity = {
                'symbol': item['symbol'],
                'composite_score': scores['composite_score'],
                'alert_level': self.get_alert_level(scores['composite_score']),
                'risk_warning': self.get_risk_warning(self.get_alert_level(scores['composite_score'])),
                'recent_mentions': item['recent_mentions'],
                'spike_ratio': item['spike_ratio'],
                'individual_scores': {
                    'reddit': scores['reddit_score'],
                    'volume': scores['volume_score'],
                    'price': scores['price_score'],
                    'sentiment': scores['sentiment_score']
                },
                'detected_at': item['detected_at'],
                'reason': f"{item['spike_ratio']:.1f}x mention spike ({item['recent_mentions']} mentions vs {item['baseline_mentions']:.1f} baseline)"
            }
            
            trending_opportunities.append(opportunity)
        
        # Sort by composite score (highest first)
        trending_opportunities.sort(key=lambda x: x['composite_score'], reverse=True)
        
        return trending_opportunities
    
    def save_trending_opportunities(self, opportunities):
        """Save trending opportunities to S3"""
        if not opportunities:
            print("No trending opportunities to save")
            return
        
        # Convert to DataFrame for easy storage
        df = pd.DataFrame(opportunities)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"trending_opportunities_{timestamp}.csv"
        
        # Upload to S3
        success = self.s3_uploader.upload_dataframe(df, filename)
        
        if success:
            print(f"✅ Saved {len(opportunities)} trending opportunities to S3: {filename}")
        else:
            print("❌ Failed to save trending opportunities to S3")
    
    def run_detection_and_save(self):
        """Main execution: detect opportunities and save results"""
        try:
            opportunities = self.detect_trending_opportunities()
            
            if opportunities:
                print(f"\nTRENDING OPPORTUNITIES DETECTED:")
                print("=" * 60)
                
                for opp in opportunities[:5]:  # Show top 5
                    print(f"{opp['symbol']} - Score: {opp['composite_score']:.2f} ({opp['alert_level']})")
                    print(f"   Reason: {opp['reason']}")
                    print(f"   {opp['risk_warning']}")
                    print()
                
                # Save to S3
                self.save_trending_opportunities(opportunities)
                
            else:
                print("No trending opportunities detected at this time")
                
        except Exception as e:
            print(f"Error in trending detection: {e}")

if __name__ == "__main__":
    detector = TrendingDetector()
    detector.run_detection_and_save()