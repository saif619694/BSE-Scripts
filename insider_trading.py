from curl_cffi import requests
from bs4 import BeautifulSoup
import json
import time
import schedule
from datetime import datetime, timedelta
import glob
import os
import csv
from io import StringIO
from typing import List, Dict, Set, Optional
from loguru import logger

class InsiderTradingScraper:
    BASE_URL = "https://www.bseindia.com/corporates/Insider_Trading_new.aspx"
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.bseindia.com',
        'Referer': 'https://www.bseindia.com/corporates/Insider_Trading_new.aspx',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
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
            session = self._create_session()
            csv_data = self._fetch_csv_data(session)
            if not csv_data:
                logger.error("Failed to fetch CSV data")
                return []
            return self._process_csv_data(csv_data)
        except Exception as e:
            logger.critical(f"Critical error in fetch_data: {str(e)}")
            return []
    
    def _create_session(self):
        session = requests.Session()
        if self.proxies:
            session.proxies = self.proxies
        return session

    def _fetch_csv_data(self, session) -> Optional[str]:
        get_response = self._make_request(
            session=session,
            method='GET',
            url=self.BASE_URL, 
            headers=self.HEADERS
        )
        if not get_response:
            logger.error("Failed initial GET request after retries")
            return None
        data = self._get_request_data(get_response.text)
        post_response = self._make_request(
            session=session,
            method='POST',
            url=self.BASE_URL, 
            data=data,
            headers={**self.HEADERS, 'Referer': self.BASE_URL},
            cookies=get_response.cookies
        )
        if not post_response:
            logger.error("Failed CSV download POST request after retries")
            return None
        return post_response.text

    def _make_request(self, session, method: str, url: str, headers: Optional[Dict] = None, 
                 data: Optional[Dict] = None, cookies: Optional[Dict] = None, 
                 json_data: Optional[Dict] = None) -> Optional[requests.Response]:
        headers = headers or self.HEADERS
        for attempt in range(1, self.retries + 1):
            try:
                response = session.request(
                    method, 
                    url, 
                    headers=headers, 
                    data=data,
                    cookies=cookies, 
                    json=json_data, 
                    timeout=60,
                    impersonate="chrome131"
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

    def _get_request_data(self, response_text: str) -> Dict:
        soup = BeautifulSoup(response_text, 'html.parser')
        data = {tag.get('name'): tag.get('value', '') for tag in soup.select('input[type="hidden"]')}
        required_fields = {
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$lnkDownload',
            'ctl00$ContentPlaceHolder1$fmdate': (datetime.now() - timedelta(days=6)).strftime('%Y%m%d'),
            'ctl00$ContentPlaceHolder1$eddate': datetime.now().strftime('%Y%m%d'),
            'ctl00$ContentPlaceHolder1$hidCurrentDate': datetime.now().strftime('%Y/%m/%d')
        }
        for field, value in required_fields.items():
            if field not in data:
                logger.warning(f"Missing required field {field} in form data")
            data[field] = value
        return data

    def _process_csv_data(self, csv_text: str) -> List[Dict]:
        results, error_count = [], 0
        try:
            csv_reader = csv.DictReader(StringIO(csv_text))
            for row_num, row in enumerate(csv_reader, 1):
                try:
                    from_date_str = row.get('Date of acquisition of shares/sale of shares/Date of Allotment(From date)', '').strip()
                    to_date_str = row.get('Date of acquisition of shares/sale of shares/Date of Allotment( To date  )', '').strip()
                    formatted_from_date = self._format_date(from_date_str, 'slash')
                    formatted_to_date = self._format_date(to_date_str, 'slash')
                    reported_date_str = row.get('Reported to Exchange', '').strip()
                    formatted_reported_date = self._format_date(reported_date_str, 'slash')

                    processed = {
                        "symbol": row.get('Security Code', '').strip(),
                        "companyName": row.get('Security Name', '').strip(),
                        "nameOfPerson": row.get('Name of Person', '').strip(),
                        "categoryOfPerson": row.get('Category of person', '').strip(),
                        "securityHeldPerTransaction": f"{row.get('Number of Securities held Prior to acquisition/Disposed', '').strip()} ({row.get('%   of  Securities held Prior to acquisition/Disposed', '').strip()})",
                        "typeOfSecurities": row.get('Type of Securities Acquired/Disposed/Pledge etc.', '').strip(),
                        "number": row.get('Number of Securities Acquired/Disposed/Pledge etc.', '').strip(),
                        "value": row.get('Value  of Securities Acquired/Disposed/Pledge etc', '').strip(),
                        "transactionType": row.get('Transaction Type ( Buy/Sale/Pledge/Revoke/Invoke)', '').strip(),
                        "securitiesHeldPostTransaction": f"{row.get('Number of Securities held Post  acquisition/Disposed/Pledge etc', '').strip()} ({row.get('Post-Transaction % of Shareholding').strip()})",
                        "period": f"{formatted_from_date} to {formatted_to_date}",
                        "modeOfAquisition": row.get('Mode of Acquisition', '').strip(),
                        "reportedToExchange": formatted_reported_date,
                        "exchange": 'bse'
                    }
                    results.append(processed)
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing row {row_num}: {str(e)}")
        except Exception as e:
            logger.critical(f"Fatal error processing CSV: {str(e)}")
        return results

    def _clean_text(self, text: str) -> str:
        return ' '.join(text.replace('\n', ' ').split())

    def _format_date(self, date_str: str, format_type: str = 'dot') -> str:
        """
        Formats a date string from 'Day Month Year' to 'DD.MM.YYYY' or 'DD/MM/YYYY'.
        Handles potential empty strings.
        """
        if not date_str:
            return ""
        try:
            date_obj = datetime.strptime(date_str.strip(), '%d %b %Y')
            if format_type == 'dot':
                return date_obj.strftime('%d.%m.%Y')
            elif format_type == 'slash':
                return date_obj.strftime('%d/%m/%Y')
            else:
                return date_str
        except ValueError:
            logger.warning(f"Could not parse date string: {date_str}")
            return date_str

    def _process_row(self, row) -> Optional[Dict]:
        cols = row.find_all('td')
        if len(cols) < 16:
            return None
        try:
            return {
                "symbol": self._clean_text(cols[0].get_text()),
                "companyName": self._clean_text(cols[1].get_text()),
                "nameOfPerson": self._clean_text(cols[2].get_text()),
                "categoryOfPerson": self._clean_text(cols[3].get_text()),
                "securityHeldPerTransaction": self._clean_text(cols[4].get_text()),
                "typeOfSecurities": self._clean_text(cols[5].get_text()),
                "number": self._clean_text(cols[6].get_text()),
                "value": self._clean_text(cols[7].get_text()),
                "transactionType": self._clean_text(cols[8].get_text()),
                "securitiesHeldPostTransaction": self._clean_text(cols[9].get_text()),
                "period": self._clean_text(cols[10].get_text()),
                "modeOfAquisition": self._clean_text(cols[11].get_text()),
                "reportedToExchange": self._clean_text(cols[15].get_text()),
                "exchange": 'bse',
                "_crawledTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "_crawler": "insider_trading"
            }
        except IndexError as e:
            logger.error(f"Error processing row: {e}")
            return None

def load_existing_entries() -> Set[str]:
    seen = set()
    for file in glob.glob("*_insider_trading.json"):
        try:
            with open(file) as f:
                data = json.load(f)
                seen.update(json.dumps(entry, sort_keys=True) for entry in data.get('entries', []))
        except (json.JSONDecodeError, IOError, KeyError) as e:
            logger.error(f"Skipping invalid file {file}: {e}")
    return seen

def manage_files(output_filename: str):
    today = datetime.today().date()
    for path in glob.glob("*_insider_trading.json"):
        if path == output_filename:
            continue
        try:
            file_date = datetime.strptime(os.path.basename(path).split('_')[0], "%Y-%m-%d").date()
            if file_date < today:
                os.remove(path)
                logger.info(f"Removed outdated file: {path}")
        except ValueError:
            continue
    for path in glob.glob("logs/insider_trading_*.log"):
        try:
            file_date_str = os.path.basename(path).split('_')[-1].split('.')[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
            if file_date < today:
                os.remove(path)
                logger.info(f"Removed outdated log file: {path}")
        except (ValueError, IndexError):
            continue

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
                logger.info(f"Uploaded {entry.get('symbol', 'unknown')} successfully")
                break
            except Exception as e:
                logger.warning(f"Upload attempt {attempt} for {entry.get('symbol', 'unknown')} failed: {str(e)}")
                if attempt < 2:
                    sleep_time = 5 * attempt
                    logger.info(f"Waiting {sleep_time}s before retry...")
                    time.sleep(sleep_time)
        if not response or response.status_code >= 400:
            success = False
            logger.error(f"Failed to upload {entry.get('symbol', 'unknown')}")
    return success


def fetch_and_save_job(proxies: Optional[Dict] = None, webhook_url: Optional[str] = None):
    logger.info("Fetching insider trading data...")
    scraper = InsiderTradingScraper(proxies=proxies)
    new_entries = scraper.fetch_data()
    logger.info(f"Found {len(new_entries)} entries on website")
    seen = load_existing_entries()
    new_entries = [e for e in new_entries if json.dumps(e, sort_keys=True) not in seen]
    if not new_entries:
        logger.info("No new entries found")
        return
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{current_date}_insider_trading.json"
    try:
        with open(output_file, 'r') as f:
            existing = json.load(f).get('entries', [])
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []
    with open(output_file, 'w') as f:
        json.dump({"entries": existing + new_entries}, f, indent=2)
    logger.info(f"Saved {len(new_entries)} new entries to {output_file}")
    if webhook_url:
        upload_success = upload_data(new_entries, webhook_url)
        logger.info("Upload completed successfully" if upload_success else "Upload failed")
    else:
        logger.warning("Webhook URL not provided, skipping upload.")


def file_management_job():
    current_date = datetime.now().strftime("%Y-%m-%d")
    manage_files(f"{current_date}_insider_trading.json")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/insider_trading_{datetime.now().strftime('%Y-%m-%d')}.log"
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
    logger.info("Insider Trading Service started. Press Ctrl+C to exit.")
    proxies = {
        "http": "",
        "https": ""
    }
    webhook_url = "http://localhost:80/insider-trading"
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