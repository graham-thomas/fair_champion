import os
import requests
from typing import Optional, Dict
from dotenv import load_dotenv
from pathlib import Path

UNPAYWALL_API = "https://api.unpaywall.org/v2/"

# Load keys from ~/.config/api_keys.env if present
load_dotenv(Path.home() / ".config/api_keys.env")
DEFAULT_UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL")


def fetch_unpaywall_record(doi: str, email: Optional[str] = None) -> Optional[Dict]:
    """
    Fetch metadata from Unpaywall for a DOI.
    If email is not provided, uses DEFAULT_UNPAYWALL_EMAIL from environment.
    """
    email_to_use = email or DEFAULT_UNPAYWALL_EMAIL
    if not email_to_use:
        raise ValueError("No Unpaywall email provided. Set UNPAYWALL_EMAIL in environment or pass as argument.")

    url = f"{UNPAYWALL_API}{doi}?email={email_to_use}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None