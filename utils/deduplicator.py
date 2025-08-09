#!/usr/bin/env python3
"""
Data Deduplication Utility
Prevents duplicate posts from being processed multiple times
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
import os

class DataDeduplicator:
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-west-2'))
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        
    def get_existing_ids(self, days_back=7):
        """Get IDs of posts from last N days to avoid duplicates"""
        existing_ids = set()
        
        try:
            # List recent files
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            
            # Check multiple prefixes for different file types
            prefixes = ['reddit_financial_', 'bluesky_financial_', 'processed-data/']
            
            for prefix in prefixes:
                try:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=prefix,
                        MaxKeys=20
                    )
                    
                    for obj in response.get('Contents', []):
                        if obj['LastModified'].replace(tzinfo=None) > cutoff_date:
                            # Download and extract IDs
                            try:
                                file_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj['Key'])
                                df = pd.read_csv(file_obj['Body'])
                                if 'id' in df.columns:
                                    existing_ids.update(df['id'].tolist())
                            except:
                                # Silently skip files we can't access
                                continue
                except:
                    # Skip this prefix if we can't access it
                    continue
                        
        except Exception as e:
            # Silently continue if deduplication fails
            pass
            
        return existing_ids
    
    def remove_duplicates(self, df):
        """Remove posts that already exist in S3"""
        if df.empty or 'id' not in df.columns:
            return df
            
        existing_ids = self.get_existing_ids()
        
        if existing_ids:
            initial_count = len(df)
            df = df[~df['id'].isin(existing_ids)]
            removed_count = initial_count - len(df)
            print(f"Removed {removed_count} duplicate posts")
            
        return df