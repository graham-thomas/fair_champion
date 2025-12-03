from typing import Dict, Any, Optional
import re

def clean_html(text: Optional[str]) -> str:
    """Remove any HTML tags from a string."""
    if not text:
        return ""
    return re.sub(r"<.*?>", "", text)

def parse_unpaywall_metadata(meta: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract consistent fields from an Unpaywall response, stripping HTML from text fields.
    """
    best_oa = meta.get("best_oa_location") or {}
    return {
        "doi": meta.get("doi"),
        "title": clean_html(meta.get("title")),
        "journal": clean_html(meta.get("journal_name")),
        "published_date": meta.get("published_date"),
        "oa_status": meta.get("oa_status"),
        "best_oa_location_url": best_oa.get("url"),
        "pdf_url": best_oa.get("url_for_pdf"),
        "xml_url": best_oa.get("url_for_landing_page"),
    }
