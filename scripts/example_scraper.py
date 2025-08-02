import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.base_scraper import BaseScraper
from utils.s3_uploader import S3Uploader
from typing import Dict, Any
import os
import time
from dotenv import load_dotenv

load_dotenv()

class ExampleScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.uploader = S3Uploader()
    
    def scrape(self) -> Dict[str, Any]:
        """Example scraping implementation"""
        urls = os.getenv('TARGET_URLS', '').split(',')
        results = []
        
        for url in urls:
            if not url.strip():
                continue
                
            try:
                soup = self.get_page(url.strip())
                data = {
                    'url': url.strip(),
                    'title': soup.find('title').text if soup.find('title') else 'No title',
                    'text_content': soup.get_text()[:1000],  # First 1000 chars
                    'scraped_at': str(time.time())
                }
                results.append(data)
            except Exception as e:
                print(f"Error scraping {url}: {e}")
        
        return {'results': results, 'total_scraped': len(results)}
    
    def run(self):
        """Execute scraping and upload to S3"""
        data = self.scrape()
        self.uploader.upload_data(data, 'example_source')

if __name__ == "__main__":
    scraper = ExampleScraper()
    scraper.run()