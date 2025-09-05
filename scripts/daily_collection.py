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
    
    # Collect Fear & Greed Index
    print("\n4. Collecting Fear & Greed Index...")
    try:
        from fear_greed_collector import FearGreedCollector
        fear_greed_collector = FearGreedCollector()
        fear_greed_collector.run_collection_and_upload()
    except Exception as e:
        print(f"Fear & Greed collection failed (non-critical): {e}")
    
    # Monthly predictions (run on 1st of month)
    print("\n5. Checking monthly predictions...")
    try:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'ai-workbench'))
        from monthly_predictor import MonthlyPredictor
        
        current_date = datetime.utcnow()
        
        # Run monthly cycle on 1st of month or if no predictions exist for next month
        predictor = MonthlyPredictor()
        history = predictor.load_predictions_history()
        
        next_month = current_date.replace(day=1)
        if current_date.month == 12:
            next_month = next_month.replace(year=current_date.year + 1, month=1)
        else:
            next_month = next_month.replace(month=current_date.month + 1)
        
        next_month_str = next_month.strftime('%Y-%m')
        
        # Check if prediction exists for next month
        has_prediction = any(p['target_month'] == next_month_str for p in history.get('predictions', []))
        
        if current_date.day == 1 or not has_prediction:
            print(f"Running monthly prediction cycle for {next_month_str}...")
            predictor.run_monthly_prediction_cycle(['BTC'])
            print("Monthly predictions updated!")
        else:
            print(f"Monthly prediction for {next_month_str} already exists")
            
    except Exception as e:
        print(f"Monthly prediction failed (non-critical): {e}")
    
    print("\nDaily collection complete!")
    print("Next: Run AI workbench to process the data")

if __name__ == "__main__":
    main()