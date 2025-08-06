#!/usr/bin/env python3
"""
Basic Options Collector
Collects just options contract information for tracking and future expansion
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader
from dotenv import load_dotenv
import time

# Load .env from project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(project_root, '.env'))

class BasicOptionsCollector:
    def __init__(self):
        self.s3_uploader = S3Uploader()
        self.polygon_key = os.getenv('POLYGON_API_KEY')
        self.base_url = "https://api.polygon.io"
        
        # Focus on most liquid options
        self.tracked_tickers = ['SPY', 'QQQ', 'AAPL', 'TSLA', 'NVDA']
    
    def get_current_price(self, symbol):
        """Get current stock price"""
        try:
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/prev"
            params = {'apikey': self.polygon_key}
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('results'):
                    return data['results'][0]['c']  # close price
            return None
        except:
            return None
    
    def get_options_contracts(self, symbol):
        """Get basic options contract info"""
        contracts = []
        
        try:
            url = f"{self.base_url}/v3/reference/options/contracts"
            params = {
                'underlying_ticker': symbol,
                'limit': 50,
                'apikey': self.polygon_key
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'OK' and 'results' in data:
                    current_price = self.get_current_price(symbol)
                    
                    for contract in data['results']:
                        strike = contract.get('strike_price', 0)
                        exp_date = contract.get('expiration_date', '')
                        
                        # Only include contracts expiring within 60 days
                        if exp_date:
                            exp_datetime = datetime.strptime(exp_date, '%Y-%m-%d')
                            days_to_exp = (exp_datetime - datetime.now()).days
                            
                            if 0 < days_to_exp <= 60:
                                contracts.append({
                                    'symbol': symbol,
                                    'ticker': contract.get('ticker'),
                                    'option_type': contract.get('contract_type', '').lower(),
                                    'strike': strike,
                                    'expiration': exp_datetime,
                                    'days_to_expiry': days_to_exp,
                                    'current_stock_price': current_price,
                                    'moneyness': strike / current_price if current_price else 0,
                                    'collected_at': datetime.utcnow(),
                                    'data_source': 'polygon_basic'
                                })
            
            time.sleep(12)  # Rate limit: 5 calls/minute
            
        except Exception as e:
            print(f"Error getting contracts for {symbol}: {e}")
        
        return contracts
    
    def collect_all_contracts(self):
        """Collect contract info for all tracked symbols"""
        all_contracts = []
        
        print(f"Collecting basic options contracts for {len(self.tracked_tickers)} symbols...")
        
        for i, symbol in enumerate(self.tracked_tickers):
            print(f"[{i+1}/{len(self.tracked_tickers)}] {symbol}...")
            contracts = self.get_options_contracts(symbol)
            all_contracts.extend(contracts)
            print(f"  Found {len(contracts)} active contracts")
        
        return pd.DataFrame(all_contracts)
    
    def run_collection_and_upload(self):
        """Main execution"""
        try:
            if not self.polygon_key:
                print("No Polygon API key found")
                return
            
            df = self.collect_all_contracts()
            
            if df.empty:
                print("No options contracts collected")
                return
            
            # Upload to S3
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"options_contracts_{timestamp}.csv"
            
            success = self.s3_uploader.upload_dataframe(df, filename)
            
            if success:
                print(f"[SUCCESS] Uploaded {len(df)} options contracts: {filename}")
                
                # Show summary
                print(f"\nSummary:")
                print(f"  Total contracts: {len(df)}")
                print(f"  Symbols: {', '.join(df['symbol'].unique())}")
                print(f"  Calls: {len(df[df['option_type'] == 'call'])}")
                print(f"  Puts: {len(df[df['option_type'] == 'put'])}")
                print(f"  Avg days to expiry: {df['days_to_expiry'].mean():.1f}")
            else:
                print("[ERROR] Failed to upload contracts")
                
        except Exception as e:
            print(f"Error in collection: {e}")

if __name__ == "__main__":
    collector = BasicOptionsCollector()
    collector.run_collection_and_upload()