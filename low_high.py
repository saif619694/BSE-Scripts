from curl_cffi import requests
import pandas as pd
import io
import time
import json
from datetime import datetime
import glob
import os
from typing import Dict, List, Optional, Set
import schedule


class BSEScraper:
    BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/HLDownloadCSVNew/w"
    HEADERS = {
        'sec-ch-ua-platform': '"macOS"',
        'Referer': 'https://www.bseindia.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
    }

    COLUMN_MAP = {
        'High': {
            'name': 'Security Name',
            'code': 'Security Code',
            'prev_value': 'Previous 52 Weeks High',
            'prev_date': 'Previous 52 Weeks High Date',
            'new_value': '52 Weeks High',
            'all_time': ('All Time High Price', 'All Time High Date')
        },
        'Low': {
            'name': 'Scrip Name',
            'code': 'Scrip Code',
            'prev_value': 'Previous 52 Weeks Low',
            'prev_date': 'Previous 52 Weeks Low Date',
            'new_value': '52 Weeks Low',
            'all_time': ('All Time Low Price', 'All Time Low Date')
        }
    }

    def __init__(self, retries: int = 3, retry_delay: int = 5):
        self.retries = retries
        self.retry_delay = retry_delay
        self.base_params = {
            'scripcode': '',
            'Grpcode': '',
            'indexcode': '',
            'EQflag': '1',
        }

    def fetch_all_data(self) -> Dict[str, List[Dict]]:
        results = {}
        for data_type in ['High', 'Low']:
            results[data_type] = self._process_data_type(data_type)
        return results

    def _process_data_type(self, data_type: str) -> List[Dict]:
        params = self.base_params.copy()
        params['HLflag'] = 'H' if data_type == 'High' else 'L'
        df = self._fetch_with_retry(params)
        return self._process_df(df, data_type) if df is not None else []

    def _fetch_with_retry(self, params: Dict) -> Optional[pd.DataFrame]:
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(self.BASE_URL, headers=self.HEADERS, params=params, timeout=30)
                response.raise_for_status()
                return pd.read_csv(io.StringIO(response.text)) if response.content else None
            except Exception as e:
                print(f"Attempt {attempt} failed ({params['HLflag']}): {e}")
                time.sleep(self.retry_delay if attempt < self.retries else 0)
        return None

    def _process_df(self, df: pd.DataFrame, data_type: str) -> List[Dict]:
        cols = self.COLUMN_MAP[data_type]
        return [self._create_entry(row, data_type, cols) for _, row in df.iterrows()]

    def _create_entry(self, row: pd.Series, data_type: str, cols: Dict) -> Dict:
        return {
            "currentPrice": row.get("LTP"),
            f"previous{data_type}": row.get(cols['prev_value']),
            f"previous{data_type}Date": row.get(cols['prev_date']),
            f"new{data_type}": row.get(cols['new_value']),
            f"allTime{data_type}": ''.join(str(row.get(field, '')) for field in cols['all_time']),
            "symbol": row.get(cols['name']),
            "bseCode": row.get(cols['code']),
            "exchange": "bse",
            "group": row.get("Group"),
            "type": data_type.lower(),
            "_crawledTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "_crawler": "52week_highlow_scraper",
        }


def load_existing_entries() -> Set[str]:
    seen = set()
    for file in glob.glob("*_52week_highlow.json"):
        try:
            with open(file) as f:
                data = json.load(f)
                seen.update(json.dumps(entry, sort_keys=True) for entry in data.get('entries', []))
        except (json.JSONDecodeError, IOError, KeyError) as e:
            print(f"Skipping invalid file {file}: {e}")
    return seen


def manage_files(output_filename: str):
    today = datetime.today().date()
    for path in glob.glob("*_52week_highlow.json"):
        if path == output_filename:
            continue
        try:
            file_date = datetime.strptime(os.path.basename(path).split('_')[0], "%Y-%m-%d").date()
            if file_date < today:
                os.remove(path)
                print(f"Removed outdated file: {path}")
        except ValueError:
            continue


def is_market_hours() -> bool:
    now = datetime.now()
    return now.weekday() < 5 and datetime.strptime("08:15", "%H:%M").time() <= now.time() <= datetime.strptime("16:45", "%H:%M").time()


def upload_data(entries: List[Dict]) -> bool:
    """Common upload function that uploads high/low entries to their respective endpoints."""
    webhook_urls = {
        "high": "http://localhost:80/fifty-week/high",
        "low": "http://localhost:80/fifty-week/low"
    }
    entries_by_type = {"high": [], "low": []}
    for entry in entries:
        entry_type = entry.get("type")
        if entry_type in entries_by_type:
            entries_by_type[entry_type].append(entry)
    print(f"Uploading high: {len(entries_by_type['high'])}, low: {len(entries_by_type['low'])} entries")
    retries, retry_delay, success = 3, 5, True
    for entry_type, type_entries in entries_by_type.items():
        if not type_entries:
            continue
        print(f"Uploading {len(type_entries)} individual {entry_type} entries")
        for entry in type_entries:
            for attempt in range(retries):
                try:
                    response = requests.post(webhook_urls[entry_type], json=entry)
                    response.raise_for_status()
                    print(f"Uploaded one {entry_type} entry successfully (attempt {attempt + 1}) - {entry['symbol']}")
                    break # Break from retry loop for this entry
                except Exception as e:
                    print(f"Failed to upload one {entry_type} entry (attempt {attempt + 1}): {e}")
                    if attempt == retries - 1:
                        success = False # Mark overall success as False if last attempt fails
                        print(f"Giving up on uploading one {entry_type} entry after {retries} attempts.")
                    time.sleep(retry_delay if attempt < retries - 1 else 0)
    return success


def fetch_and_save_job():
    if not is_market_hours():
        return
    scraper = BSEScraper()
    data = scraper.fetch_all_data()
    all_entries = [entry for entries in data.values() for entry in entries]
    print(f"Found {len(all_entries)} entries on website")
    seen = load_existing_entries()
    new_entries = [e for e in all_entries if json.dumps(e, sort_keys=True) not in seen]
    print(f"Identified {len(new_entries)} new entries")
    print("-" * 100) if not new_entries else None
    if not new_entries:
        return
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{current_date}_52week_highlow.json"
    existing_entries_today = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                existing_data = json.load(f)
                existing_entries_today = existing_data.get('entries', [])
        except Exception as e:
            print(f"Error loading today's file: {e}")
    combined_entries = existing_entries_today + new_entries
    with open(output_file, 'w') as f:
        json.dump({"entries": combined_entries}, f, indent=2)
    print(f"[{datetime.now()}] Saved {len(new_entries)} new entries to {output_file}")
    success = upload_data(new_entries)
    if success:
        print("__" * 100)
    else:
        print(f"Failed to upload at least some data after multiple attempts.")


def file_management_job():
    if not is_market_hours():
        return
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{current_date}_52week_highlow.json"
    manage_files(output_file)


def main():
    schedule.every(2).minutes.do(fetch_and_save_job)
    schedule.every().hour.do(file_management_job)
    print("52week HighLow Service started. Press Ctrl+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n52 week HighLow Service stopped.")


if __name__ == "__main__":
    main()