#!/usr/bin/env python3
"""
Quick Price Updates - Every 5 minutes
Fast price collection without heavy processing
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

def quick_price_update():
    s3_uploader = S3Uploader()
    timestamp = datetime.utcnow()
    
    # Fast crypto prices (single API call)
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            'ids': 'bitcoin,ethereum,monero,litecoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true'
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        price_data = []
        for coin_id, coin_data in data.items():
            symbol_map = {
                'bitcoin': 'BTC',
                'ethereum': 'ETH',
                'monero': 'XMR', 
                'litecoin': 'LTC'
            }
            symbol = symbol_map.get(coin_id, coin_id.upper())
            price_data.append({
                'timestamp': timestamp,
                'symbol': symbol,
                'price': coin_data['usd'],
                'change_24h': coin_data.get('usd_24h_change', 0),
                'data_type': 'quick_price'
            })
        
        df = pd.DataFrame(price_data)
        filename = f"quick_prices_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
        
        if s3_uploader.upload_dataframe(df, filename):
            print(f"✅ Quick price update: BTC ${data['bitcoin']['usd']:.0f}, ETH ${data['ethereum']['usd']:.0f}")
        else:
            print("❌ Upload failed")
            
    except Exception as e:
        print(f"❌ Quick price update failed: {e}")

if __name__ == "__main__":
    quick_price_update()