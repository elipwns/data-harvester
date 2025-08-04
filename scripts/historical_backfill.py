#!/usr/bin/env python3
"""
Historical Data Backfill
Collects years of Bitcoin network data, price history, and monetary data
"""

import requests
import pandas as pd
from datetime import datetime
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv

load_dotenv()

class HistoricalDataCollector:
    def __init__(self):
        self.s3_uploader = S3Uploader()
    
    def collect_bitcoin_network_history(self):
        """Collect complete Bitcoin network history from 2009-present"""
        print("Collecting Bitcoin network history...")
        
        metrics = {
            'total-bitcoins': 'Total BTC Supply',
            'market-cap': 'Market Cap',
            'hash-rate': 'Hash Rate', 
            'difficulty': 'Mining Difficulty',
            'n-unique-addresses': 'Active Addresses',
            'n-transactions': 'Daily Transactions'
        }
        
        all_data = []
        
        for metric, description in metrics.items():
            print(f"  Fetching {description}...")
            try:
                url = f"https://api.blockchain.info/charts/{metric}?timespan=all&format=json"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                for point in data['values']:
                    all_data.append({
                        'date': pd.to_datetime(point['x'], unit='s').date(),
                        'metric': metric,
                        'value': point['y'],
                        'description': description,
                        'category': 'bitcoin_network',
                        'collected_at': datetime.utcnow()
                    })
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                print(f"    Error fetching {metric}: {e}")
        
        return pd.DataFrame(all_data)
    
    def collect_bitcoin_price_history(self):
        """Collect Bitcoin price history from CoinGecko (using free tier)"""
        print("Collecting Bitcoin price history (last 365 days)...")
        
        try:
            # Use 365 days instead of max to avoid auth requirements
            url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            price_data = []
            
            # Process price data
            for timestamp, price in data['prices']:
                price_data.append({
                    'date': pd.to_datetime(timestamp, unit='ms').date(),
                    'metric': 'price',
                    'value': price,
                    'description': 'BTC Price USD',
                    'category': 'bitcoin_price',
                    'collected_at': datetime.utcnow()
                })
            
            return pd.DataFrame(price_data)
            
        except Exception as e:
            print(f"Error fetching Bitcoin price history: {e}")
            return pd.DataFrame()
    
    def collect_crypto_market_data(self):
        """Collect Ethereum price history (last 365 days)"""
        print("Collecting Ethereum price history (last 365 days)...")
        
        try:
            url = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=365"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            eth_data = []
            
            for timestamp, price in data['prices']:
                eth_data.append({
                    'date': pd.to_datetime(timestamp, unit='ms').date(),
                    'metric': 'eth_price',
                    'value': price,
                    'description': 'ETH Price USD',
                    'category': 'ethereum_price',
                    'collected_at': datetime.utcnow()
                })
            
            return pd.DataFrame(eth_data)
            
        except Exception as e:
            print(f"Error fetching Ethereum history: {e}")
            return pd.DataFrame()
    
    def collect_us_monetary_data(self):
        """Collect US monetary supply data from FRED (requires API key)"""
        print("Collecting US monetary data...")
        
        # Note: FRED API requires free registration at https://fred.stlouisfed.org/docs/api/api_key.html
        fred_api_key = os.getenv('FRED_API_KEY')
        if not fred_api_key:
            print("  Skipping - FRED_API_KEY not found in .env file")
            print("  Get free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
            return pd.DataFrame()
        
        monetary_data = []
        
        # Key monetary indicators
        series = {
            'M1SL': 'M1 Money Supply',
            'M2SL': 'M2 Money Supply',
            'WALCL': 'Fed Balance Sheet'
        }
        
        for series_id, description in series.items():
            print(f"  Fetching {description}...")
            try:
                url = "https://api.stlouisfed.org/fred/series/observations"
                params = {
                    'series_id': series_id,
                    'api_key': fred_api_key,
                    'file_type': 'json',
                    'observation_start': '2009-01-01'  # Bitcoin era (2009-present)
                }
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                for obs in data['observations']:
                    if obs['value'] != '.':
                        monetary_data.append({
                            'date': pd.to_datetime(obs['date']).date(),
                            'metric': series_id,
                            'value': float(obs['value']),
                            'description': description,
                            'category': 'us_monetary',
                            'collected_at': datetime.utcnow()
                        })
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"    Error fetching {series_id}: {e}")
        
        return pd.DataFrame(monetary_data)
    
    def run_historical_backfill(self):
        """Main execution: collect all historical data"""
        print("=== Historical Data Backfill Started ===")
        start_time = datetime.utcnow()
        
        all_historical_data = []
        
        # Collect Bitcoin network data (2009-present)
        btc_network = self.collect_bitcoin_network_history()
        if not btc_network.empty:
            all_historical_data.append(btc_network)
            print(f"Bitcoin network: {len(btc_network)} records")
        
        # Collect Bitcoin price data (2013-present)
        btc_price = self.collect_bitcoin_price_history()
        if not btc_price.empty:
            all_historical_data.append(btc_price)
            print(f"Bitcoin price: {len(btc_price)} records")
        
        # Collect Ethereum data (last 365 days)
        eth_data = self.collect_crypto_market_data()
        if not eth_data.empty:
            all_historical_data.append(eth_data)
            print(f"Ethereum price: {len(eth_data)} records")
        
        # Collect US monetary data (last 5 years)
        monetary_data = self.collect_us_monetary_data()
        if not monetary_data.empty:
            all_historical_data.append(monetary_data)
            print(f"US monetary data: {len(monetary_data)} records")
        
        if all_historical_data:
            # Combine all data
            combined_df = pd.concat(all_historical_data, ignore_index=True)
            
            # Upload to S3
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"historical_data_{timestamp}.csv"
            
            success = self.s3_uploader.upload_dataframe(combined_df, filename)
            
            if success:
                duration = datetime.utcnow() - start_time
                print(f"Historical backfill complete!")
                print(f"Total records: {len(combined_df):,}")
                print(f"Duration: {duration}")
                print(f"File: {filename}")
            else:
                print("Failed to upload historical data")
        else:
            print("No historical data collected")

if __name__ == "__main__":
    collector = HistoricalDataCollector()
    collector.run_historical_backfill()