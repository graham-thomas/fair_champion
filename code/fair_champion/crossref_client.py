import requests
import logging
import time
from urllib.parse import quote

logger = logging.getLogger(__name__)

CROSSREF_API = "https://api.crossref.org/works/"
REQUEST_DELAY = 0.5  # Seconds between requests to avoid rate limiting

_last_request_time = 0

def _rate_limit():
    """Enforce minimum delay between requests to avoid 429 errors."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()

def fetch_crossref_json(doi, timeout=30, max_retries=3):
    url = CROSSREF_API + quote(doi, safe="")
    headers = {"Accept": "application/json"}
    
    for attempt in range(max_retries):
        try:
            _rate_limit()
            r = requests.get(url, headers=headers, timeout=timeout)
            
            if r.status_code == 429:  # Rate limited
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1  # Exponential backoff: 2s, 5s, 9s
                    logger.warning(f"Crossref: {doi} rate limited (429), waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Crossref: {doi} rate limited after {max_retries} retries")
                    return None
            
            r.raise_for_status()
            data = r.json().get("message", {})
            if data:
                logger.debug(f"Crossref: {doi} fetched successfully")
                return data
            else:
                logger.debug(f"Crossref: {doi} no message in response")
                return None
        except requests.RequestException as e:
            logger.warning(f"Crossref: {doi} request failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(1)  # Brief delay before retry
    
    return None
