import requests
from urllib.parse import quote
from pathlib import Path
from . import PROXY_DOI_TEMPLATE, EZPROXY_PREFIX

def try_doi_resolver_xml(doi, timeout=30):
    url = f"https://doi.org/{quote(doi, safe='')}"
    headers = {"Accept": "application/xml, text/xml, application/xhtml+xml"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200 and r.text.strip().startswith("<?xml"):
            return r.text
    except requests.RequestException:
        pass
    return None

def try_doi_resolver_pdf(doi, out_pdf_path: Path, timeout=30):
    url = f"https://doi.org/{quote(doi, safe='')}"
    headers = {"Accept": "application/pdf"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        ctype = r.headers.get("Content-Type", "")
        if r.status_code == 200 and ctype.startswith("application/pdf") and r.content:
            with open(out_pdf_path, "wb") as fh:
                fh.write(r.content)
            return out_pdf_path
    except requests.RequestException:
        pass

    # Try proxy template if provided
    if PROXY_DOI_TEMPLATE:
        try:
            proxy_url = PROXY_DOI_TEMPLATE % url
            r = requests.get(proxy_url, headers=headers, timeout=timeout, allow_redirects=True)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and ctype.startswith("application/pdf") and r.content:
                with open(out_pdf_path, "wb") as fh:
                    fh.write(r.content)
                return out_pdf_path
        except requests.RequestException:
            pass

    # Try EZPROXY prefix (naive)
    if EZPROXY_PREFIX:
        try:
            proxied = EZPROXY_PREFIX.rstrip("/") + "/" + url.replace("https://", "")
            r = requests.get(proxied, headers=headers, timeout=timeout, allow_redirects=True)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and ctype.startswith("application/pdf") and r.content:
                with open(out_pdf_path, "wb") as fh:
                    fh.write(r.content)
                return out_pdf_path
        except requests.RequestException:
            pass

    return None
