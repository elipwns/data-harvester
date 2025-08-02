import boto3
import json
from datetime import datetime
from typing import Dict, Any
import os

class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
    
    def upload_data(self, data: Dict[Any, Any], source: str) -> bool:
        """Upload scraped data to S3 with timestamp and source info"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            key = f"raw-data/{source}/{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            print(f"Data uploaded to s3://{self.bucket_name}/{key}")
            return True
        except Exception as e:
            print(f"Upload failed: {e}")
            return False