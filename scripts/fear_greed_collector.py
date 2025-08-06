#!/usr/bin/env python3
"""
Crypto Fear & Greed Index Collector
Collects the official Fear & Greed Index from Alternative.me
"""

import requests
import pandas as pd
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.s3_uploader import S3Uploader

class FearGreedCollector:
    def __init__(self):
        self.s3_uploader = S3Uploader()
        # Ensure bucket name is loaded
        if not self.s3_uploader.bucket_name:
            self.s3_uploader.bucket_name = 'automated-trading-data-bucket'
        self.api_url = "https://api.alternative.me/fng/"
    
    def get_current_fear_greed(self):
        """Get current Fear & Greed Index"""
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            data = response.json()
            
            if data['data']:
                current = data['data'][0]
                return {
                    'timestamp': datetime.utcnow(),
                    'fear_greed_value': int(current['value']),
                    'fear_greed_classification': current['value_classification'],
                    'data_source': 'alternative.me',
                    'next_update': current.get('time_until_update', 'Unknown')
                }
        except Exception as e:
            print(f"Error fetching Fear & Greed Index: {e}")
            return None
    
    def get_historical_fear_greed(self, days=30):
        """Get historical Fear & Greed Index"""
        try:
            url = f"{self.api_url}?limit={days}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            historical_data = []
            for item in data['data']:
                historical_data.append({
                    'date': datetime.fromtimestamp(int(item['timestamp'])).date(),
                    'fear_greed_value': int(item['value']),
                    'fear_greed_classification': item['value_classification'],
                    'data_source': 'alternative.me'
                })
            
            return historical_data
        except Exception as e:
            print(f"Error fetching historical Fear & Greed Index: {e}")
            return []
    
    def run_collection_and_upload(self):
        """Collect current Fear & Greed Index and upload to S3"""
        try:
            current_data = self.get_current_fear_greed()
            
            if not current_data:
                print("No Fear & Greed data collected")
                return
            
            df = pd.DataFrame([current_data])
            print(f"Collected Fear & Greed Index: {current_data['fear_greed_value']} ({current_data['fear_greed_classification']})")
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"fear_greed_index_{timestamp}.csv"
            
            # Upload to S3
            success = self.s3_uploader.upload_dataframe(df, filename)
            
            if success:
                print(f"Successfully uploaded Fear & Greed data to S3: {filename}")
            else:
                print("Failed to upload Fear & Greed data to S3")
                
        except Exception as e:
            print(f"Error in Fear & Greed collection: {e}")

if __name__ == "__main__":
    collector = FearGreedCollector()
    collector.run_collection_and_upload()