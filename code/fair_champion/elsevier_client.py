import requests
import logging
from . import ELSEVIER_KEY

logger = logging.getLogger(__name__)

ELSEVIER_ARTICLE_API = "https://api.elsevier.com/content/article/doi/"

def fetch_elsevier_xml(doi, timeout=20):
    if not ELSEVIER_KEY:
        logger.debug(f"Elsevier: API key not configured")
        return None
    headers = {"X-ELS-APIKey": ELSEVIER_KEY, "Accept": "application/xml"}
    url = ELSEVIER_ARTICLE_API + requests.utils.requote_uri(doi)
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 404:
            logger.debug(f"Elsevier: {doi} not found (404)")
            return None
        r.raise_for_status()
        logger.debug(f"Elsevier: {doi} fetched successfully")
        return r.text
    except requests.RequestException as e:
        logger.warning(f"Elsevier: {doi} request failed: {e}")
        return None
