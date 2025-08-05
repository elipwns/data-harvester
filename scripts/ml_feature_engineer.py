#!/usr/bin/env python3
"""
ML Feature Engineering
Prepares data for price prediction models by creating technical indicators,
sentiment features, and time-based features
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
import boto3
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

class MLFeatureEngineer:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_uploader = S3Uploader()
    
    def load_price_data(self, days=30):
        """Load recent price data from S3"""
        try:
            objects = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="raw-data/price_data_"
            )
            
            if not objects.get('Contents'):
                return pd.DataFrame()
            
            # Get recent files
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
                return combined_df.sort_values('timestamp')
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error loading price data: {e}")
            return pd.DataFrame()
    
    def load_sentiment_data(self, days=30):
        """Load recent sentiment data from S3"""
        try:
            objects = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="processed-data/"
            )
            
            if not objects.get('Contents'):
                return pd.DataFrame()
            
            # Get most recent processed file
            latest = sorted(objects['Contents'], 
                          key=lambda x: x['LastModified'], 
                          reverse=True)[0]
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=latest['Key']
            )
            
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter to recent data
            cutoff_date = datetime.now() - timedelta(days=days)
            return df[df['timestamp'] >= cutoff_date]
            
        except Exception as e:
            print(f"Error loading sentiment data: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, price_df, symbol):
        """Calculate technical indicators for a specific symbol"""
        symbol_df = price_df[price_df['symbol'] == symbol].copy()
        symbol_df = symbol_df.sort_values('timestamp').reset_index(drop=True)
        
        if len(symbol_df) < 21:  # Need minimum data for indicators
            return symbol_df
        
        # Simple Moving Averages
        symbol_df['sma_7'] = symbol_df['price'].rolling(window=7).mean()
        symbol_df['sma_21'] = symbol_df['price'].rolling(window=21).mean()
        
        # Exponential Moving Averages
        symbol_df['ema_12'] = symbol_df['price'].ewm(span=12).mean()
        symbol_df['ema_26'] = symbol_df['price'].ewm(span=26).mean()
        
        # MACD
        symbol_df['macd'] = symbol_df['ema_12'] - symbol_df['ema_26']
        symbol_df['macd_signal'] = symbol_df['macd'].ewm(span=9).mean()
        symbol_df['macd_histogram'] = symbol_df['macd'] - symbol_df['macd_signal']
        
        # RSI
        delta = symbol_df['price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        symbol_df['rsi'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        bb_window = 20
        bb_std = 2
        symbol_df['bb_middle'] = symbol_df['price'].rolling(window=bb_window).mean()
        bb_std_dev = symbol_df['price'].rolling(window=bb_window).std()
        symbol_df['bb_upper'] = symbol_df['bb_middle'] + (bb_std_dev * bb_std)
        symbol_df['bb_lower'] = symbol_df['bb_middle'] - (bb_std_dev * bb_std)
        symbol_df['bb_position'] = (symbol_df['price'] - symbol_df['bb_lower']) / (symbol_df['bb_upper'] - symbol_df['bb_lower'])
        
        # Price momentum features
        symbol_df['price_change_1d'] = symbol_df['price'].pct_change(1)
        symbol_df['price_change_3d'] = symbol_df['price'].pct_change(3)
        symbol_df['price_change_7d'] = symbol_df['price'].pct_change(7)
        
        # Volume features (if available)
        if 'volume_24h' in symbol_df.columns:
            symbol_df['volume_sma_7'] = symbol_df['volume_24h'].rolling(window=7).mean()
            symbol_df['volume_ratio'] = symbol_df['volume_24h'] / symbol_df['volume_sma_7']
        
        return symbol_df
    
    def calculate_sentiment_features(self, sentiment_df):
        """Calculate sentiment-based features"""
        if sentiment_df.empty or 'sentiment_label' not in sentiment_df.columns:
            return pd.DataFrame()
        
        # Convert sentiment labels to numeric scores
        sentiment_map = {
            '1 star': 1, '2 stars': 2, '3 stars': 3, '4 stars': 4, '5 stars': 5
        }
        sentiment_df['sentiment_numeric'] = sentiment_df['sentiment_label'].map(sentiment_map)
        
        # Daily sentiment aggregation
        sentiment_df['date'] = sentiment_df['timestamp'].dt.date
        daily_sentiment = sentiment_df.groupby('date').agg({
            'sentiment_numeric': ['mean', 'std', 'count'],
            'sentiment_score': ['mean', 'std']
        }).reset_index()
        
        # Flatten column names
        daily_sentiment.columns = ['date', 'sentiment_mean', 'sentiment_std', 'post_count', 
                                 'confidence_mean', 'confidence_std']
        
        # Calculate sentiment momentum
        daily_sentiment['sentiment_momentum_3d'] = daily_sentiment['sentiment_mean'].rolling(3).mean()
        daily_sentiment['sentiment_momentum_7d'] = daily_sentiment['sentiment_mean'].rolling(7).mean()
        
        # Sentiment volatility
        daily_sentiment['sentiment_volatility'] = daily_sentiment['sentiment_std'].rolling(7).mean()
        
        # Post volume features
        daily_sentiment['post_volume_ma7'] = daily_sentiment['post_count'].rolling(7).mean()
        daily_sentiment['post_volume_ratio'] = daily_sentiment['post_count'] / daily_sentiment['post_volume_ma7']
        
        return daily_sentiment
    
    def create_time_features(self, df):
        """Create time-based features"""
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['day_of_month'] = df['timestamp'].dt.day
        df['month'] = df['timestamp'].dt.month
        df['quarter'] = df['timestamp'].dt.quarter
        
        # Market hours (US market: 9:30 AM - 4:00 PM EST)
        df['is_market_hours'] = ((df['hour'] >= 14) & (df['hour'] <= 21)).astype(int)  # UTC
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        return df
    
    def create_target_variables(self, df, symbol):
        """Create target variables for ML prediction"""
        symbol_df = df[df['symbol'] == symbol].copy()
        symbol_df = symbol_df.sort_values('timestamp').reset_index(drop=True)
        
        # Future price targets (what we want to predict)
        symbol_df['target_1h'] = symbol_df['price'].shift(-1)  # Next data point
        symbol_df['target_1d'] = symbol_df['price'].shift(-24)  # 24 hours ahead (if hourly data)
        symbol_df['target_3d'] = symbol_df['price'].shift(-72)  # 3 days ahead
        symbol_df['target_7d'] = symbol_df['price'].shift(-168)  # 7 days ahead
        
        # Target returns (percentage change)
        symbol_df['target_return_1h'] = (symbol_df['target_1h'] / symbol_df['price'] - 1) * 100
        symbol_df['target_return_1d'] = (symbol_df['target_1d'] / symbol_df['price'] - 1) * 100
        symbol_df['target_return_3d'] = (symbol_df['target_3d'] / symbol_df['price'] - 1) * 100
        symbol_df['target_return_7d'] = (symbol_df['target_7d'] / symbol_df['price'] - 1) * 100
        
        # Binary classification targets (up/down)
        symbol_df['target_direction_1h'] = (symbol_df['target_return_1h'] > 0).astype(int)
        symbol_df['target_direction_1d'] = (symbol_df['target_return_1d'] > 0).astype(int)
        symbol_df['target_direction_3d'] = (symbol_df['target_return_3d'] > 0).astype(int)
        symbol_df['target_direction_7d'] = (symbol_df['target_return_7d'] > 0).astype(int)
        
        return symbol_df
    
    def merge_features(self, price_df, sentiment_df):
        """Merge price and sentiment features"""
        if sentiment_df.empty:
            return price_df
        
        # Convert timestamp to date for merging
        price_df['date'] = price_df['timestamp'].dt.date
        
        # Merge with sentiment features
        merged_df = price_df.merge(sentiment_df, on='date', how='left')
        
        # Forward fill sentiment features for missing dates
        sentiment_cols = [col for col in sentiment_df.columns if col != 'date']
        merged_df[sentiment_cols] = merged_df[sentiment_cols].ffill()
        
        return merged_df
    
    def create_ml_dataset(self, symbol='BTC', days=30):
        """Create complete ML dataset for a symbol"""
        print(f"Creating ML dataset for {symbol}...")
        
        # Load data
        price_df = self.load_price_data(days)
        sentiment_df = self.load_sentiment_data(days)
        
        if price_df.empty:
            print("No price data available")
            return pd.DataFrame()
        
        # Calculate technical indicators
        price_features = self.calculate_technical_indicators(price_df, symbol)
        
        # Calculate sentiment features
        sentiment_features = self.calculate_sentiment_features(sentiment_df)
        
        # Merge features
        ml_df = self.merge_features(price_features, sentiment_features)
        
        # Add time features
        ml_df = self.create_time_features(ml_df)
        
        # Create target variables
        ml_df = self.create_target_variables(ml_df, symbol)
        
        # Remove rows with NaN targets (can't predict future for latest data)
        ml_df = ml_df.dropna(subset=['target_1d', 'target_return_1d'])
        
        print(f"Created ML dataset with {len(ml_df)} samples and {len(ml_df.columns)} features")
        
        return ml_df
    
    def save_ml_dataset(self, ml_df, symbol):
        """Save ML dataset to S3"""
        if ml_df.empty:
            print("No ML dataset to save")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"ml_features_{symbol}_{timestamp}.csv"
        
        success = self.s3_uploader.upload_dataframe(ml_df, filename)
        
        if success:
            print(f"✅ Saved ML dataset to S3: {filename}")
        else:
            print("❌ Failed to save ML dataset")
    
    def run_feature_engineering(self, symbols=['BTC']):
        """Main execution: create ML datasets for specified symbols"""
        print("=== ML Feature Engineering Started ===")
        
        for symbol in symbols:
            try:
                ml_df = self.create_ml_dataset(symbol)
                if not ml_df.empty:
                    self.save_ml_dataset(ml_df, symbol)
                    
                    # Print feature summary
                    print(f"\n{symbol} Feature Summary:")
                    print(f"  Samples: {len(ml_df)}")
                    print(f"  Features: {len(ml_df.columns)}")
                    print(f"  Date range: {ml_df['timestamp'].min()} to {ml_df['timestamp'].max()}")
                    
                    # Show feature correlation with target
                    if 'target_return_1d' in ml_df.columns:
                        numeric_cols = ml_df.select_dtypes(include=[np.number]).columns
                        correlations = ml_df[numeric_cols].corr()['target_return_1d'].abs().sort_values(ascending=False)
                        print(f"  Top correlated features:")
                        for feature, corr in correlations.head(5).items():
                            if feature != 'target_return_1d':
                                print(f"    {feature}: {corr:.3f}")
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
        
        print("\n=== Feature Engineering Complete ===")

if __name__ == "__main__":
    engineer = MLFeatureEngineer()
    engineer.run_feature_engineering(['BTC'])