import sys
from pathlib import Path
import csv
import docx
import os
from dotenv import load_dotenv

from code.dois import extract_dois
from code.fetch import fetch_unpaywall_record
from code.parse import parse_unpaywall_metadata
from code.downloader import Downloader

# -------------------------
# Configuration
# Load keys from ~/.config/api_keys.env if present
# -------------------------
load_dotenv(Path.home() / ".config/api_keys.env")
ELSEVIER_KEY = os.getenv("ELSEVIER_API_KEY")
UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL")  # optional


def extract_text_from_docx(path: Path) -> str:
    doc = docx.Document(path)
    return "\n".join(paragraph.text for paragraph in doc.paragraphs)


def main(input_docx: str):
    input_path = Path(input_docx)
    out_csv = Path("analysis/get_data") / f"{input_path.stem}_data.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    text = extract_text_from_docx(input_path)

    # Deduplicate DOIs while preserving order
    dois = list(dict.fromkeys(extract_dois(text)))

    dl = Downloader()
    rows = []

    for doi in dois:
        meta = fetch_unpaywall_record(doi, UNPAYWALL_EMAIL)
        if not meta:
            print(f"ERROR: metadata not found for {doi}")
            continue

        parsed = parse_unpaywall_metadata(meta)
        parsed_doi = parsed["doi"] or doi

        # Download PDF
        pdf_url = parsed.get("pdf_url")
        if pdf_url:
            pdf_path = Path("analysis/get_data/pdfs") / f"{parsed_doi.replace('/', '_')}.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            success = dl.download_pdf(pdf_url, pdf_path)
            if not success:
                print(f"WARNING: PDF download failed for {doi}")
        else:
            print(f"WARNING: No PDF URL for {doi}")

        # Download XML
        xml_url = parsed.get("xml_url")
        if xml_url:
            xml_path = Path("analysis/get_data/xml") / f"{parsed_doi.replace('/', '_')}.xml"
            xml_path.parent.mkdir(parents=True, exist_ok=True)
            success = dl.download_xml(xml_url, xml_path)
            if not success:
                print(f"WARNING: XML download failed for {doi}")
        else:
            print(f"WARNING: No XML URL for {doi}")

        rows.append(parsed)

    if rows:
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV saved → {out_csv}")
    else:
        print("No data to write to CSV.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python code/get_data/get_data.py <input_docx>")
    else:
        main(sys.argv[1])
