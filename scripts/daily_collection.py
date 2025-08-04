#!/usr/bin/env python3
"""
Daily Data Collection
Runs both Reddit sentiment scraping and price collection
"""

from reddit_scraper import RedditScraper
from price_collector import PriceCollector
from datetime import datetime

def main():
    print(f"=== Daily Data Collection - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    # Collect Reddit sentiment data
    print("\n1. Collecting Reddit sentiment data...")
    reddit_scraper = RedditScraper()
    reddit_scraper.run_scrape_and_upload()
    
    # Collect Bluesky data
    print("\n2. Collecting Bluesky sentiment data...")
    try:
        from bluesky_scraper import BlueskyScraper
        bluesky_scraper = BlueskyScraper()
        bluesky_scraper.run_scrape_and_upload()
    except Exception as e:
        print(f"Bluesky collection failed (non-critical): {e}")
    
    # Collect price data
    print("\n3. Collecting price data...")
    price_collector = PriceCollector()
    price_collector.run_collection_and_upload()
    
    print("\nDaily collection complete!")
    print("Next: Run AI workbench to process the data")

if __name__ == "__main__":
    main()