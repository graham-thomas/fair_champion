import requests
import logging
from . import SPRINGER_META_KEY, SPRINGER_OA_KEY

logger = logging.getLogger(__name__)

# Springer has two separate APIs with different keys:
# 1. Metadata API (SPRINGER_META_API_KEY) - uses /meta/v2/ endpoints
# 2. Open Access API (SPRINGER_OPEN_ACCESS_API_KEY) - uses /openaccess/ endpoints
SPRINGER_META_URL = "https://api.springernature.com/meta/v2/json"
SPRINGER_OA_URL = "https://api.springernature.com/openaccess/json"
SPRINGER_OA_XML = "https://api.springernature.com/openaccess/xml"

def fetch_springer_meta(doi, timeout=20):
    """Fetch metadata using the Metadata API with SPRINGER_META_API_KEY."""
    if not SPRINGER_META_KEY:
        logger.debug(f"Springer Meta: API key not configured")
        return None
    params = {"q": f"doi:{doi}", "api_key": SPRINGER_META_KEY}
    try:
        r = requests.get(SPRINGER_META_URL, params=params, timeout=timeout)
        if r.status_code == 401:
            logger.warning(f"Springer Meta: Authentication failed - API key may be invalid. Please verify SPRINGER_OA_API_KEY in ~/.config/api_keys.env")
            return None
        r.raise_for_status()
        data = r.json()
        records = data.get("records") or []
        if records:
            logger.debug(f"Springer Meta: {doi} found")
            return records[0]
        else:
            logger.debug(f"Springer Meta: {doi} not found in records")
            return None
    except requests.RequestException as e:
        logger.warning(f"Springer Meta: {doi} request failed: {e}")
        return None

def fetch_springer_oa_json(doi, timeout=20):
    if not SPRINGER_OA_KEY:
        logger.debug(f"Springer OA JSON: API key not configured")
        return None
    params = {"q": f"doi:{doi}", "api_key": SPRINGER_OA_KEY}
    try:
        r = requests.get(SPRINGER_OA_URL, params=params, timeout=timeout)
        if r.status_code == 401:
            logger.debug(f"Springer OA JSON: Authentication failed - API key may be invalid")
            return None
        r.raise_for_status()
        data = r.json()
        records = data.get("records") or []
        if records:
            logger.debug(f"Springer OA JSON: {doi} found")
            return records[0]
        else:
            logger.debug(f"Springer OA JSON: {doi} not found in records")
            return None
    except requests.RequestException as e:
        logger.warning(f"Springer OA JSON: {doi} request failed: {e}")
        return None

def fetch_springer_oa_xml(doi, timeout=20):
    """Attempt to fetch Springer OA XML via the openaccess/xml endpoint."""
    if not SPRINGER_OA_KEY:
        logger.debug(f"Springer OA XML: API key not configured")
        return None
    params = {"q": f"doi:{doi}", "api_key": SPRINGER_OA_KEY}
    try:
        r = requests.get(SPRINGER_OA_XML, params=params, timeout=timeout)
        if r.status_code == 401:
            logger.debug(f"Springer OA XML: Authentication failed - API key may be invalid")
            return None
        r.raise_for_status()
        if r.text:
            logger.debug(f"Springer OA XML: {doi} fetched successfully")
            return r.text
        else:
            logger.debug(f"Springer OA XML: {doi} empty response")
            return None
    except requests.RequestException as e:
        logger.warning(f"Springer OA XML: {doi} request failed: {e}")
        return None
