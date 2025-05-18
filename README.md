# SaifScripts

This repository contains Python scripts for scraping financial data from BSE India.

## Scripts

### [`announcements.py`](announcements.py)
This script is designed to scrape company announcements from BSE India. It includes functionality to download and process PDF attachments associated with the announcements. The script runs on a schedule to fetch new announcements periodically.

### [`low_high.py`](low_high.py)
This script scrapes 52-week high and low price data for securities listed on BSE India. It saves the fetched data to a JSON file and includes logic to manage and clean up older data files. It also attempts to upload the new data via a webhook.

### [`volume.py`](volume.py)
This script focuses on scraping volume data, specifically spurt volume, from BSE India. It saves the data to a JSON file, manages old files and logs, and uploads the new data via a webhook.

### [`insider_trading.py`](insider_trading.py)
This script is used to scrape insider trading data from BSE India. It fetches the data, saves it to a JSON file, and includes features for managing outdated files and logs. New data is also uploaded via a webhook.
