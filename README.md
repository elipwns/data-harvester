# Data Harvester

Web scraping and data collection pipeline for automated trading system.

## Part of 3-Repo System

- **[Data Harvester](https://github.com/elipwns/data-harvester)** ← You are here
- **[AI Workbench](https://github.com/elipwns/ai-workbench)** - Financial sentiment analysis
- **[Insight Dashboard](https://github.com/elipwns/insight-dashboard)** - Data visualization

## Current Status

✅ **Working Components:**
- Web scraping with BeautifulSoup
- S3 data upload functionality
- Terraform AWS infrastructure
- Serverless deployment configuration

## Quick Start

1. **Set up AWS infrastructure:**
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

3. **Run scraper:**
   ```bash
   python3 scripts/example_scraper.py
   ```

## Architecture

- **`scripts/base_scraper.py`** - Base scraping functionality
- **`scripts/example_scraper.py`** - Example implementation
- **`utils/s3_uploader.py`** - S3 upload operations
- **`terraform/`** - AWS S3 bucket infrastructure
- **`aws/serverless.yml`** - Lambda deployment config

## Data Flow

1. Scrape target websites
2. Upload raw data → S3 `raw-data/`
3. Trigger AI processing pipeline

## Requirements

- Python 3.10+
- AWS credentials configured
- Terraform (for infrastructure)
- Target URLs in `.env`
