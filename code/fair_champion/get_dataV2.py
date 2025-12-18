"""
get_data.py

Main entry point for FAIR Champion analysis.
Resolves DOIs, extracts data availability information from HTML,
and scores papers against BioFAIR principles.
"""

import sys
import csv
from pathlib import Path
from datetime import datetime

from .resolver import resolve_doi
from .doi_extract import extract_from_docx
from .fair_scoring import score_fair
from .html_extract import (
    extract_data_availability_statement,
    extract_dataset_dois,
    extract_data_formats,
    extract_data_licenses,
)
from .metadata_extract import extract_title_authors_journal


# ----------------------------
# Core processing
# ----------------------------

def process_docx(docx_path: Path):
    entries = extract_from_docx(docx_path)

    if not entries:
        sys.exit("❌ No DOIs found in document")

    results = []

    for entry in entries:
        doi = entry["doi"]
        print(f"🔍 Processing DOI: {doi}")

        resolved = resolve_doi(doi)

        title = authors = journal = None
        das = None
        data_dois = []
        formats = []
        licenses = []

        # ----------------------------
        # Metadata (best-effort)
        # ----------------------------
        if resolved.html:
            title, authors, journal = extract_title_authors_journal(resolved.html)

        # ----------------------------
        # FAIR-relevant extraction
        # ----------------------------
        if resolved.html:
            das = extract_data_availability_statement(resolved.html)
            data_dois = extract_dataset_dois(resolved.html)
            formats = extract_data_formats(resolved.html)
            licenses = extract_data_licenses(resolved.html)

        fair_score = score_fair(
            data_dois=data_dois,
            formats=formats,
            licenses=licenses,
        )

        results.append({
            "paper_doi": doi,
            "title": title,
            "authors": authors,
            "journal": journal,
            "data_availability_statement": das,
            "dataset_dois": "; ".join(data_dois) if data_dois else None,
            "data_formats": "; ".join(formats) if formats else None,
            "data_licenses": "; ".join(licenses) if licenses else None,
            "fair_score": fair_score,
            "used_rendered_html": resolved.used_renderer,
        })

    return results


# ----------------------------
# Output
# ----------------------------

def write_output(results, input_path: Path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{timestamp}_{input_path.stem}.csv"
    out_path = Path("analysis") / out_name
    out_path.parent.mkdir(exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\n💾 Results written to {out_path}")

    # Champion
    champion = max(results, key=lambda r: r["fair_score"])
    print("\n🏆 BioFAIR Champion")
    print(f"Title: {champion['title']}")
    print(f"Journal: {champion['journal']}")
    print(f"DOI: {champion['paper_doi']}")
    print(f"FAIR score: {champion['fair_score']}")


# ----------------------------
# CLI entry point
# ----------------------------

def main(argv=None):
    argv = argv or sys.argv

    if len(argv) != 2:
        sys.exit("Usage: python -m code.fair_champion <input.docx>")

    input_path = Path(argv[1])

    if not input_path.exists():
        sys.exit(f"❌ File not found: {input_path}")

    if input_path.suffix.lower() != ".docx":
        sys.exit("❌ Input file must be a .docx document")

    results = process_docx(input_path)
    write_output(results, input_path)


if __name__ == "__main__":
    main()
