from curl_cffi import requests
import json
import time
import schedule
import os
import glob
from datetime import datetime
from typing import List, Dict, Set, Optional
from loguru import logger

class VolumeScraper:
    BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/SpurtvolumeNew/w?flag=1"
    HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://www.bseindia.com',
        'Referer': 'https://www.bseindia.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    def __init__(self, retries: int = 3, retry_delay: int = 10, proxies: Optional[Dict] = None):
        self.retries = retries
        self.retry_delay = retry_delay
        self.proxies = proxies or {}

    def fetch_data(self) -> List[Dict]:
        try:
            response = self._make_request('GET', self.BASE_URL)
            if not response:
                logger.error("Failed to fetch volume data after retries")
                return []
            return self._process_data(response.json())
        except Exception as e:
            logger.critical(f"Critical error in fetch_data: {str(e)}")
            return []

    def _make_request(self, method: str, url: str, headers: Optional[Dict] = None,
                    data: Optional[Dict] = None, cookies: Optional[Dict] = None,
                    json_data: Optional[Dict] = None) -> Optional[requests.Response]:
        headers = headers or self.HEADERS
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.request(
                    method, url, headers=headers, data=data, proxies=self.proxies,
                    cookies=cookies, json=json_data, timeout=60
                )
                response.raise_for_status()
                return response
            except Exception as e:
                if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                    logger.warning(f"Attempt {attempt} failed ({method} {url}). Status: {e.response.status_code}")
                    if e.response.status_code in [502, 503, 504]:
                        logger.info("Server-side error detected, retrying...")
                    else:
                        break
                else:
                    logger.warning(f"Attempt {attempt} failed ({method} {url}): {str(e)}")
            
            if attempt < self.retries:
                sleep_time = self.retry_delay * attempt
                logger.info(f"Waiting {sleep_time}s before retry...")
                time.sleep(sleep_time)
        
        logger.error(f"All {self.retries} attempts failed for {method} {url}")
        return None

    def _process_data(self, data_json: List[Dict]) -> List[Dict]:
        return [{
            "symbol": item.get('scrip_cd', '').strip(),
            "company": item.get('scripname', '').strip(),
            "todayVolume": item.get('Trd_vol', '').strip(),
            "twoWeekAvgVolume": item.get('wkavgqty', '').strip(),
            "volumeChange": item.get('volumechangetimes', '').strip(),
            "turnover": item.get('TurnOver', '').strip(),
            "change": item.get('change_val', '').strip(),
            "ltp": item.get('Ltradert', '').strip(),
            "changePer": item.get('change_percent', '').strip(),
            "exchange": 'bse',
            "_crawledTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "_crawler": "volume_scraper",
        } for item in data_json]

def load_existing_entries() -> Set[str]:
    seen = set()
    for file in glob.glob("*_volume.json"):
        try:
            with open(file) as f:
                data = json.load(f)
                seen.update(json.dumps(entry, sort_keys=True) for entry in data.get('entries', []))
        except (json.JSONDecodeError, IOError, KeyError) as e:
            logger.error(f"Skipping invalid file {file}: {e}")
    return seen

def manage_files(output_filename: str):
    today = datetime.today().date()
    for path in glob.glob("*_volume.json"):
        if path == output_filename:
            continue
        try:
            file_date = datetime.strptime(os.path.basename(path).split('_')[0], "%Y-%m-%d").date()
            if file_date < today:
                os.remove(path)
                logger.info(f"Removed outdated file: {path}")
        except ValueError:
            continue
    
    for path in glob.glob("logs/volume_*.log"):
        try:
            file_date_str = os.path.basename(path).split('_')[-1].split('.')[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
            if file_date < today:
                os.remove(path)
                logger.info(f"Removed outdated log file: {path}")
        except (ValueError, IndexError):
            continue

def is_market_hours() -> bool:
    now = datetime.now()
    return now.weekday() < 5 and datetime.strptime("08:15", "%H:%M").time() <= now.time() <= datetime.strptime("16:45", "%H:%M").time()

def upload_data(entries: List[Dict], webhook_url: str) -> bool:
    session = requests.Session()
    success = True
    for entry in entries:
        response = None
        for attempt in range(1, 3):
            try:
                response = session.post(
                    webhook_url,
                    json=entry,
                    timeout=30
                )
                response.raise_for_status()
                logger.info(f"Uploaded {entry.get('company', 'unknown')} successfully")
                break
            except Exception as e:
                logger.warning(f"Upload attempt {attempt} for {entry.get('symbol', 'unknown')} failed: {str(e)}")
                if attempt < 2:
                    sleep_time = 5 * attempt
                    logger.info(f"Waiting {sleep_time}s before retry...")
                    time.sleep(sleep_time)
        if not response or response.status_code >= 400:
            success = False
            logger.error(f"Failed to upload {entry.get('company', 'unknown')}")
    return success

def fetch_and_save_job(proxies: Optional[Dict] = None, webhook_url: Optional[str] = None):
    if not is_market_hours():
        return
    logger.info("Fetching volume data...")
    scraper = VolumeScraper(proxies=proxies)
    all_entries = scraper.fetch_data()
    logger.info(f"Found {len(all_entries)} entries on website")

    seen = load_existing_entries()
    new_entries = [e for e in all_entries if json.dumps(e, sort_keys=True) not in seen]

    if not new_entries:
        logger.info("No new entries found")
        return

    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{current_date}_volume.json"

    existing = []
    try:
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                file_content = f.read()
                if file_content:
                    existing = json.loads(file_content).get('entries', [])
                else:
                    logger.warning(f"Existing file {output_file} is empty.")
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error reading existing file {output_file}: {e}")
        existing = []

    try:
        with open(output_file, 'w') as f:
            json.dump({"entries": existing + new_entries}, f, indent=2, sort_keys=True)
        logger.info(f"Saved {len(new_entries)} new entries to {output_file}")
    except IOError as e:
        logger.error(f"Error writing to output file {output_file}: {e}")

    if webhook_url:
        upload_success = upload_data(new_entries, webhook_url)
        logger.info("Upload completed successfully" if upload_success else "Upload failed")
    else:
        logger.warning("Webhook URL not provided, skipping upload.")

def file_management_job():
    if not is_market_hours():
        return
    current_date = datetime.now().strftime("%Y-%m-%d")
    manage_files(f"{current_date}_volume.json")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/volume_{datetime.now().strftime('%Y-%m-%d')}.log"
    logger.remove()
    logger.add(
        log_file,
        rotation="1 day",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
        level="INFO"
    )
    return logger

def main():
    setup_logging()
    logger.info("Volume Scraper Service started. Press Ctrl+C to exit.")
    
    proxies = {
        "http": "",
        "https": ""
    }
    webhook_url = "http://localhost:80/volume-data"

    fetch_and_save_job(proxies=proxies, webhook_url=webhook_url)
    file_management_job()
    schedule.every(5).minutes.do(fetch_and_save_job, proxies=proxies, webhook_url=webhook_url)
    schedule.every().hour.do(file_management_job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Service stopped.")

if __name__ == "__main__":
    main()