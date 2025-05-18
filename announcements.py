import fitz
from curl_cffi import requests
from datetime import datetime
import random
import time
import re
import threading
from typing import List, Dict, Optional


class PDFProcessor:
    @staticmethod
    def convert(pdf_url: str, scraper: 'Scraper') -> Optional[str]:
        try:
            response = scraper.make_request(pdf_url, "PDFProcessor")
            if not response:
                return None

            pdf_document = fitz.open(stream=response.content, filetype="pdf")
            return "\n\n".join(
                f"Page {i+1}:\n{pdf_document.load_page(i).get_text()}"
                for i in range(pdf_document.page_count)
            )
        except Exception as e:
            print(f"PDF Conversion Error: {e}")
            return None

class Parser:
    @staticmethod
    def parse_entry(entry: Dict, scraper: 'Scraper') -> Optional[Dict]:
        if not entry.get('SLONGNAME', '').strip():
            print(f"Skipping NEWSID: {entry['NEWSID']} - Empty SLONGNAME")
            return None

        pdf_url, pdf_text = None, None
        if entry["ATTACHMENTNAME"]:
            pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{entry['ATTACHMENTNAME']}"
            pdf_text = PDFProcessor.convert(pdf_url, scraper)

        data = {
            "TEXT": pdf_text,
            "HEADLINE": entry["NEWSSUB"],
            "DETAIL": entry["MORE"] or entry["HEADLINE"].rstrip('.'),
            "SYMBOL": entry["SCRIP_CD"],
            "BROADCAST_DATE_TIME": entry["DissemDT"],
            "ATTACHMENT": pdf_url,
            "NEWS_TYPE": entry["CATEGORYNAME"],
            "SUB_CAT_TYPE": entry["SUBCATNAME"],
            "EXCHANGE": "bse",
            "COMPANY_NAME": entry["SLONGNAME"],
            "AUDIO_VIDEO_FILE": entry["AUDIO_VIDEO_FILE"],
            "SUB_TYPE": entry["NEWSSUB"],
            "NEWS_ID": entry["NEWSID"],
            "NS_URL": entry["NSURL"],
            "isAttachmentEmpty": not bool(entry["ATTACHMENTNAME"]),
            "INSERTED_ON": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        Parser._process_headline(data)
        Parser._categorize_news(data)
        return data

    @staticmethod
    def _process_headline(data: Dict) -> None:
        if data["HEADLINE"]:
            parts = re.split(r"\s-\s", data["HEADLINE"])
            if parts:
                data["COMPANY_NAME"] = parts[0].strip() if not parts[0].startswith('-') else None
                if len(parts) > 1:
                    data["SYMBOL"] = parts[1].strip()
                if len(parts) > 2:
                    data["SUB_TYPE"] = ' '.join(parts[2:]).strip()

    @staticmethod
    def _categorize_news(data: Dict) -> None:
        if "Transcript" in data.get("DETAIL", ""):
            data.update({"NEWS_TYPE": "Earnings Call Transcript", "SUB_CAT_TYPE": "Earnings Call Transcript"})
        elif "audio recording" in data.get("DETAIL", "").lower():
            data.update({"NEWS_TYPE": "Audio Recording", "SUB_CAT_TYPE": "Audio Recording"})
        elif data.get("SUB_CAT_TYPE") in [
            "Postal Ballot", "Allotment of ESOP / ESPS", "Allotment of Equity Shares",
            "Analyst / Investor Meet", "New Listing", "Publication"
        ]:
            data["NEWS_TYPE"] = data["SUB_CAT_TYPE"]
        elif data.get("SUB_CAT_TYPE") in ["Investor Presentation", "Reg. 34 (1) Annual Report"] or \
             data.get("NEWS_TYPE") in ["Earnings Call Transcript", "Audio Recording"]:
            data["NEWS_TYPE"] = "Analytical Updates"
        else:
            data.setdefault("NEWS_TYPE", "Others")

class Scraper:
    HEADERS = {
        'sec-ch-ua-platform': '"macOS"',
        'Referer': 'https://www.bseindia.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
    }

    def __init__(self, proxies: Dict = None):
        self.proxies = proxies or {}
        self.base_url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

    def make_request(self, url: str, context: str, headers: Dict = None, retries: int = 2) -> Optional[requests.Response]:
        for _ in range(retries):
            try:
                response = requests.get(
                    url,
                    headers=headers or self.HEADERS,
                    proxies=self.proxies,
                    timeout=100
                )
                if response.status_code == 200:
                    return response
                print(f"Retrying {url}")
                time.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                print(f"Request Error ({context}): {e}")
                time.sleep(3)
        print(f"Failed to get {url} after {retries} retries")
        return None

    def get_pagination(self) -> int:
        current_date = datetime.now().strftime('%Y%m%d')
        url = f"{self.base_url}?pageno=1&strCat=-1&strPrevDate={current_date}&strScrip=&strSearch=P&strToDate={current_date}&strType=C&subcategory=-1"
        response = self.make_request(url, "Pagination")
        return int(response.json()['Table'][0]['TotalPageCnt']) if response else 1

    def scrape_page(self, page: int, existing_attachments: List[str]) -> List[Dict]:
        print(f"Getting page {page}") 
        current_date = datetime.now().strftime('%Y%m%d')
        url = f"{self.base_url}?pageno={page}&strCat=-1&strPrevDate={current_date}&strScrip=&strSearch=P&strToDate={current_date}&strType=C&subcategory=-1"
        response = self.make_request(url, "ScrapePage")
        if not response:
            return []

        entries = []
        for entry in response.json().get('Table', []):
            news_id = entry['NEWSID']
            if news_id in existing_attachments:
                continue

            if parsed := Parser.parse_entry(entry, self):
                entries.append(parsed)
        return entries

    def scrape_job(self, existing_attachments: List[str], pagination: bool = False) -> List[Dict]:
        max_pages = self.get_pagination() if pagination else 1
        return [
            entry
            for page in range(1, min(max_pages, 70) + 1)
            for entry in self.scrape_page(page, existing_attachments)
        ]

class ScraperScheduler:
    def __init__(self, scraper: Scraper, get_existing_url: str, upload_data_url: str):
        self.scraper = scraper
        self.last_paginated_run = time.time()
        self.get_existing_url = get_existing_url
        self.upload_data_url = upload_data_url

    def _get_existing_attachments(self, retries: int = 3, retry_delay: int = 5) -> List[str]:
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        for attempt in range(retries):
            try:
                response = requests.post(self.get_existing_url, json={"date": current_date_str})
                response.raise_for_status()
                print(f"Successfully fetched existing attachments on attempt {attempt + 1}.")
                return response.json().get('newsIds', [])
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to fetch existing attachments: {e}")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
        print(f"Failed to fetch existing attachments after {retries} attempts.")
        return []

    def _upload_data(self, data: List[Dict], retries: int = 3, retry_delay: int = 5) -> None:
        if not data:
            print("No new entries to upload.")
            return

        for attempt in range(retries):
            try:
                response = requests.post(self.upload_data_url, json=data)
                response.raise_for_status()
                print(f"Successfully uploaded {len(data)} entries on attempt {attempt + 1}.")
                return
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to upload data : {e}")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
        print(f"Failed to upload data after {retries} attempts.")


    def _run_interval(self, pagination: bool) -> bool:
        try:
            existing = self._get_existing_attachments()
            print(f"Existing attachments: {len(existing)}")
            data = self.scraper.scrape_job(existing, pagination)
            self._upload_data(data)
            print("-" * 100)
            return True
        except Exception as e:
            print(f"Run interval failed: {e}")
            print("-" * 100)
            return False


    def start(self):
        def job_loop():
            while True:
                print("2 minutes run")
                # Run every 2 minutes without pagination
                success_2min = self._run_interval(False)
                if not success_2min:
                    print("2 minutes run failed, will retry next interval.")

                time.sleep(120)

                # Run every 30 minutes with pagination
                if time.time() - self.last_paginated_run >= 1800:
                    print("30 minutes run")
                    success_30min = self._run_interval(True)
                    if success_30min:
                        self.last_paginated_run = time.time()
                    else:
                        print("30 minutes run failed, will retry next interval.")

        thread = threading.Thread(target=job_loop, daemon=True)
        thread.start()
        thread.join()

def main():
    # Configuration
    PROXIES = {
        "http": "",
        "https": "",
    }
    GET_EXISTING_URL = "https://dummy.online/check" # Dummy URL for getting existing attachments
    UPLOAD_DATA_URL = "https://duplicate.whalesbook.online/check" # Dummy URL for uploading data


    # Initialize components
    scraper = Scraper(proxies=PROXIES)
    scheduler = ScraperScheduler(scraper, GET_EXISTING_URL, UPLOAD_DATA_URL)

    # Start scraping process
    scheduler.start()


if __name__ == '__main__':
    start = time.time()
    main()
    print(f"Runtime: {time.time() - start:.2f} seconds")
    