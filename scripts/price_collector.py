#!/usr/bin/env python3
"""
Price Data Collector
Collects current prices for major assets to correlate with sentiment
"""

import requests
import pandas as pd
from datetime import datetime
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv

load_dotenv()

class PriceCollector:
    def __init__(self):
        self.s3_uploader = S3Uploader()
        
        # Assets to track by category
        self.assets = {
            'CRYPTO': {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'XMR': 'monero',
                'LTC': 'litecoin',
                'CRYPTO_MARKET_CAP': 'total_market_cap'
            },
            'US_STOCKS': {
                'SPY': 'SPY',  # S&P 500 ETF
                'QQQ': 'QQQ',  # NASDAQ ETF
                'VTI': 'VTI',  # Total Stock Market ETF
                'TSLA': 'TSLA' # Tesla for Tesla Watch page
            }
        }
    
    def get_crypto_prices(self) -> dict:
        """Get crypto prices from CoinGecko with enhanced ML features"""
        crypto_data = {}
        
        try:
            # Enhanced CoinGecko API call with more data
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': 'bitcoin,ethereum,monero,litecoin',
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_change': 'true',
                'include_24hr_vol': 'true',
                'include_last_updated_at': 'true'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Format crypto data with ML features
            for coin_id, coin_data in data.items():
                symbol_map = {
                    'bitcoin': 'BTC',
                    'ethereum': 'ETH', 
                    'monero': 'XMR',
                    'litecoin': 'LTC'
                }
                symbol = symbol_map.get(coin_id, coin_id.upper())
                price = coin_data['usd']
                volume_24h = coin_data.get('usd_24h_vol', 0)
                change_24h = coin_data.get('usd_24h_change', 0)
                
                crypto_data[symbol] = {
                    'price': price,
                    'market_cap': coin_data.get('usd_market_cap', 0),
                    'change_24h': change_24h,
                    'volume_24h': volume_24h,
                    'volatility': abs(change_24h),  # Simple volatility measure
                    'volume_price_ratio': volume_24h / price if price > 0 else 0,
                    'last_updated': coin_data.get('last_updated_at', 0)
                }
                
        except Exception as e:
            print(f"Error fetching crypto prices: {e}")
        
        return crypto_data
    
    def get_stock_prices(self) -> dict:
        """Get stock prices from Yahoo Finance with rate limiting"""
        stock_data = {}
        
        try:
            # Using Yahoo Finance API (free, unofficial)
            symbols = ['SPY', 'QQQ', 'VTI', 'TSLA']
            
            for i, symbol in enumerate(symbols):
                # Add longer delay between requests to avoid rate limits
                if i > 0:
                    import time
                    time.sleep(5)  # Increased delay
                
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.get(url, headers=headers)
                
                if response.status_code == 429:
                    print(f"Rate limited for {symbol}, waiting 30 seconds...")
                    import time
                    time.sleep(30)
                    # Retry once
                    response = requests.get(url, headers=headers)
                    if response.status_code == 429:
                        print(f"Still rate limited for {symbol}, skipping...")
                        continue
                    
                response.raise_for_status()
                data = response.json()
                
                if 'chart' in data and data['chart']['result']:
                    result = data['chart']['result'][0]
                    current_price = result['meta']['regularMarketPrice']
                    prev_close = result['meta']['previousClose']
                    change_pct = ((current_price - prev_close) / prev_close) * 100
                    
                    stock_data[symbol] = {
                        'price': current_price,
                        'change_24h': change_pct,
                        'prev_close': prev_close
                    }
                    
        except Exception as e:
            print(f"Error fetching stock prices: {e}")
            # Continue with crypto data even if stocks fail
        
        return stock_data
    
    def collect_all_prices(self) -> pd.DataFrame:
        """Collect enhanced price data with ML features"""
        timestamp = datetime.utcnow()
        all_data = []
        
        # Get crypto prices with enhanced features
        crypto_prices = self.get_crypto_prices()
        for symbol, data in crypto_prices.items():
            all_data.append({
                'timestamp': timestamp,
                'category': 'CRYPTO',
                'symbol': symbol,
                'price': data['price'],
                'change_24h': data['change_24h'],
                'volume_24h': data.get('volume_24h', 0),
                'volatility': data.get('volatility', 0),
                'volume_price_ratio': data.get('volume_price_ratio', 0),
                'market_cap': data.get('market_cap', 0),
                'data_type': 'price'
            })
        
        # Get stock prices
        stock_prices = self.get_stock_prices()
        for symbol, data in stock_prices.items():
            all_data.append({
                'timestamp': timestamp,
                'category': 'US_STOCKS',
                'symbol': symbol,
                'price': data['price'],
                'change_24h': data['change_24h'],
                'volume_24h': 0,  # Not available from Yahoo Finance simple API
                'volatility': abs(data['change_24h']),
                'volume_price_ratio': 0,
                'market_cap': 0,  # Not applicable for ETFs
                'data_type': 'price'
            })
        
        return pd.DataFrame(all_data)
    
    def run_collection_and_upload(self):
        """Main execution: collect prices and upload to S3"""
        try:
            # Collect price data
            df = self.collect_all_prices()
            
            if df.empty:
                print("No price data collected")
                return
            
            print(f"Collected prices for {len(df)} assets")
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"price_data_{timestamp}.csv"
            
            # Upload to S3
            success = self.s3_uploader.upload_dataframe(df, filename)
            
            if success:
                print(f"Successfully uploaded price data to S3: {filename}")
            else:
                print("Failed to upload price data to S3")
                
        except Exception as e:
            print(f"Error in price collection: {e}")

if __name__ == "__main__":
    collector = PriceCollector()
    collector.run_collection_and_upload()