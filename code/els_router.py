#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv

# -------------------------------
# Environment
# -------------------------------
load_dotenv(os.path.expanduser("~/.config/api_keys.env"))
ELSEVIER_KEY = os.getenv("ELSEVIER_API_KEY")

if not ELSEVIER_KEY:
    raise SystemExit("Error: ELSEVIER_API_KEY is not set.")

HEADERS = {"Accept": "application/json"}

# -------------------------------
# 1. Identify publisher via Crossref
# -------------------------------
def get_publisher(doi: str) -> str:
    url = f"https://api.crossref.org/works/{doi}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()["message"]
        return data.get("publisher", "Unknown")
    except Exception:
        return "Unknown"


# -------------------------------
# 2. Elsevier API lookup
# -------------------------------
def fetch_elsevier(doi):
    url = f"https://api.elsevier.com/content/article/doi/{doi}"
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/xml"
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    return r.text      # return raw XML


# -------------------------------
# 3. Springer API lookup (BioMed Central, SpringerLink)
# -------------------------------
def fetch_springer(doi: str):
    # The free Springer Metadata API uses query=doi:<DOI>
    api_key = os.getenv("SPRINGER_API_KEY")
    if not api_key:
        return {"error": "Springer API key missing"}

    url = "https://api.springernature.com/metadata/json"
    params = {"q": f"doi:{doi}", "api_key": api_key}

    r = requests.get(url, params=params, timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


# -------------------------------
# 4. Wiley API (metadata only)
# -------------------------------
def fetch_wiley(doi: str):
    api_key = os.getenv("WILEY_API_KEY")
    if not api_key:
        return {"error": "Wiley API key missing"}

    url = f"https://api.wiley.com/onlinelibrary/tdm/v1/articles/{doi}"
    headers = {"apikey": api_key}

    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


# -------------------------------
# 5. Routing logic
# -------------------------------
def fetch_by_doi(doi: str):
    publisher = get_publisher(doi).lower()

    # ---- Elsevier ----
    if "elsevier" in publisher or "cell press" in publisher or "lancet" in publisher:
        data = fetch_elsevier(doi)
        if data is None:
            return {"doi": doi, "publisher": publisher, "status": "not_elsevier"}
        return {"doi": doi, "publisher": publisher, "status": "success", "data": data}

    # ---- Springer / BMC ----
    if "springer" in publisher or "biomed central" in publisher:
        data = fetch_springer(doi)
        return {"doi": doi, "publisher": publisher, "status": "success", "data": data}

    # ---- Wiley ----
    if "wiley" in publisher:
        data = fetch_wiley(doi)
        return {"doi": doi, "publisher": publisher, "status": "success", "data": data}

    # ---- Unsupported publishers ----
    return {
        "doi": doi,
        "publisher": publisher,
        "status": "unsupported_publisher",
    }


# -------------------------------
# 6. Command-line interface
# -------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python els_router.py <DOI>")
        raise SystemExit()

    doi = sys.argv[1]
    result = fetch_by_doi(doi)
    print(result)
