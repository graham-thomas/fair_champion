import requests
from pathlib import Path
from typing import Optional
import csv

LOG_FILE = Path("analysis/get_data/download_log.csv")

class Downloader:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        # Initialize CSV log with header if not exists
        if not LOG_FILE.exists():
            LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with LOG_FILE.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["url", "type", "status_code", "content_type", "success"])

    def _log(self, url: str, dtype: str, status_code: int, content_type: str, success: bool):
        with LOG_FILE.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url, dtype, status_code, content_type, success])

    def download_pdf(self, url: str, outpath: Path) -> bool:
        try:
            r = self.session.get(url, timeout=15)
            content_type = r.headers.get("content-type", "")
            success = r.status_code == 200 and content_type.lower().startswith("application/pdf")
            self._log(url, "PDF", r.status_code, content_type, success)
            if success:
                outpath.write_bytes(r.content)
                return True
            else:
                print(f"WARNING: PDF download failed for {url} → HTTP {r.status_code}, content-type: {content_type}")
        except requests.RequestException as e:
            self._log(url, "PDF", 0, "", False)
            print(f"ERROR: PDF download exception for {url} → {e}")
        return False

    def download_xml(self, url: str, outpath: Path) -> bool:
        try:
            r = self.session.get(url, timeout=15)
            content_type = r.headers.get("content-type", "")
            success = r.status_code == 200 and "xml" in content_type.lower()
            self._log(url, "XML", r.status_code, content_type, success)
            if success:
                outpath.write_bytes(r.content)
                return True
            else:
                print(f"WARNING: XML download failed for {url} → HTTP {r.status_code}, content-type: {content_type}")
        except requests.RequestException as e:
            self._log(url, "XML", 0, "", False)
            print(f"ERROR: XML download exception for {url} → {e}")
        return False
