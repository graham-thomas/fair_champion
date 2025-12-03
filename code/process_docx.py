#!/usr/bin/env python3
import sys
import re
from docx import Document
from els_router import fetch_by_doi

DOI_REGEX = r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+"

def extract_dois(docx_path):
    doc = Document(docx_path)
    text = "\n".join([p.text for p in doc.paragraphs])
    return re.findall(DOI_REGEX, text)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_docx.py <file.docx>")
        raise SystemExit()

    path = sys.argv[1]

    dois = extract_dois(path)
    if not dois:
        print("No DOIs found.")
        raise SystemExit()

    for doi in dois:
        print(f"\nProcessing DOI: {doi}")
        result = fetch_by_doi(doi)
        print(result)
