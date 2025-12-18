import requests
from . import WILEY_KEY

WILEY_TDM_ENDPOINT = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"

def fetch_wiley_tdm_xml(doi, timeout=20):
    if not WILEY_KEY:
        return None
    url = WILEY_TDM_ENDPOINT + doi
    headers = {"apikey": WILEY_KEY, "Accept": "application/vnd.wiley.article+xml"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    except requests.RequestException:
        return None
