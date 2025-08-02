import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from typing import Dict, Any
import time
import random

class BaseScraper(ABC):
    def __init__(self, delay_range=(1, 3)):
        self.session = requests.Session()
        self.delay_range = delay_range
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_page(self, url: str) -> BeautifulSoup:
        """Fetch and parse a webpage"""
        time.sleep(random.uniform(*self.delay_range))
        response = self.session.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    
    @abstractmethod
    def scrape(self) -> Dict[str, Any]:
        """Implement scraping logic for specific source"""
        pass