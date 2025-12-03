#!/usr/bin/env python3
import os
import csv
import requests
from pathlib import Path
from docx import Document
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# --- Load API key ---
load_dotenv(Path.home() / ".config/api_keys.env")
API_KEY = os.getenv("ELSEVIER_API_KEY")
if not API_KEY:
    raise ValueError("ELSEVIER_API_KEY not found in ~/.config/api_keys.env")

# --- Logging ---
def log_error(logfile, message):
    with open(logfile, "a", encoding="utf-8") as lf:
        lf.write(message.rstrip() + "\n")

# --- Functions ---
def read_papers(docx_file):
    """Extract paper titles and DOIs from a docx file."""
    doc = Document(docx_file)
    papers = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if text:
            parts = text.split()
            doi = parts[-1]
            title = ' '.join(parts[:-1])
            papers.append({'title': title, 'doi': doi})
    return papers

def query_elsevier_api(doi, accept="application/xml"):
    """Query Elsevier Article API for a single DOI."""
    url = f"https://api.elsevier.com/content/article/doi/{doi}"
    headers = {"X-ELS-APIKey": API_KEY, "Accept": accept}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def save_xml(doi, xml_text, output_dir):
    """Save XML text to a file named by DOI."""
    safe_doi = doi.replace('/', '_')
    xml_path = output_dir / f"{safe_doi}.xml"
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(xml_text)
    return xml_path

def parse_das_from_xml(xml_file):
    """Extract DAS and hyperlinks from Elsevier XML."""
    tree = ET.parse(xml_file)
    root = tree.getroot()

    ns = {k: v for k, v in [node for _, node in ET.iterparse(xml_file, events=['start-ns'])]}

    das_text = ""
    links = []

    da_section = root.find('.//ce:data-availability', ns)
    if da_section is not None:
        paras = da_section.findall('.//ce:para', ns)
        das_text = ' '.join([''.join(p.itertext()).strip() for p in paras])
        for p in paras:
            for link in p.findall('.//ce:inter-ref', ns):
                href = link.attrib.get('{http://www.w3.org/1999/xlink}href')
                if href:
                    links.append(href)

    return das_text, links

def parse_article_metadata(xml_file):
    """Extract authors, journal, OA metadata, and corresponding author from XML."""
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract namespaces
    ns = {k: v for k, v in [node for _, node in ET.iterparse(xml_file, events=['start-ns'])]}

    # ---------- Journal ----------
    journal = root.find('.//ce:publication-name', ns)
    if journal is None:
        journal = root.find('.//prism:publicationName', ns)
    journal_text = journal.text if journal is not None else ""

    # ---------- Authors ----------
    authors_elems = root.findall('.//ce:author', ns)
    authors = []
    corresponding_author = ""
    corresponding_email = ""

    for a in authors_elems:
        given = a.find('ce:given-name', ns)
        surname = a.find('ce:surname', ns)

        name = ""
        if given is not None and given.text:
            name += given.text + " "
        if surname is not None and surname.text:
            name += surname.text
        if name.strip():
            authors.append(name.strip())

        # Check for cross-reference to correspondence
        cross_refs = a.findall('.//ce:cross-ref', ns)
        for ref in cross_refs:
            if ref.attrib.get('refid', '').startswith('cor'):
                corresponding_author = name.strip()

        # Email address (rarely present in author elements)
        email_elem = a.find('.//ce:e-address', ns)
        if email_elem is not None and email_elem.text:
            corresponding_email = email_elem.text.strip()
            if not corresponding_author:
                corresponding_author = name.strip()

    authors_text = ", ".join(authors)

    # ---------- Open Access Metadata ----------
    # These elements are in <coredata> with the default namespace
    default_ns = 'http://www.elsevier.com/xml/svapi/article/dtd'
    ns_oa = {'def': default_ns}
    
    def get_text_oa(tag_name):
        elem = root.find(f'.//def:{tag_name}', ns_oa)
        return elem.text.strip() if elem is not None and elem.text else ""

    oa_article         = get_text_oa('openaccessArticle')
    oa_type            = get_text_oa('openaccessType')
    oa_user_license    = get_text_oa('openaccessUserLicense')

    return {
        "authors": authors_text,
        "journal": journal_text,
        "corresponding_author": corresponding_author,
        "corresponding_email": corresponding_email,
        "openaccessArticle": oa_article,
        "openaccessType": oa_type,
        "openaccessUserLicense": oa_user_license
    }
# --- Main ---
def main(input_docx):
    papers = read_papers(input_docx)

    now = datetime.now()
    folder_name = f"{now.year}_{now.month:02d}"
    output_dir = Path("analysis/els_client") / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = now.strftime("%Y%m%d_%H%M%S")
    log_file = output_dir / f"{timestamp}_processing.log"

    csv_path = output_dir / (Path(input_docx).stem + "_data.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
        'title', 'doi', 'authors', 'journal',
        'corresponding_author', 'corresponding_email',
        'openaccessArticle', 'openaccessType',
        'openaccessUserLicense',
        'data_availability_statement', 'data_links'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for paper in papers:
            doi = paper['doi']
            title = paper['title']
            print(f"Processing DOI: {doi} ...")

            try:
                xml_text = query_elsevier_api(doi)
                xml_file = save_xml(doi, xml_text, output_dir)

                das_text, links = parse_das_from_xml(xml_file)
                meta = parse_article_metadata(xml_file)

                # --- Logging conditions ---
                if not das_text:
                    log_error(log_file, f"{doi}: No DAS section found.")

                if not meta['authors']:
                    log_error(log_file, f"{doi}: No authors found.")

                if not meta['journal']:
                    log_error(log_file, f"{doi}: No journal name found.")

                if not meta['corresponding_author']:
                    log_error(log_file, f"{doi}: No corresponding author found.")

                if not meta['corresponding_email']:
                    log_error(log_file, f"{doi}: No corresponding author email found.")

                if not meta['openaccessArticle']:
                    log_error(log_file, f"{doi}: No openaccessArticle status found.")

                if not meta['openaccessType']:
                    log_error(log_file, f"{doi}: No openaccessType found.")

                if not meta['openaccessUserLicense']:
                    log_error(log_file, f"{doi}: No openaccessUserLicense found.")

                if not links:
                    log_error(log_file, f"{doi}: No data links found.")

                writer.writerow({
                    'title': title,
                    'doi': doi,
                    'authors': meta['authors'],
                    'journal': meta['journal'],
                    'corresponding_author': meta['corresponding_author'],
                    'corresponding_email': meta['corresponding_email'],
                    'openaccessArticle': meta['openaccessArticle'],
                    'openaccessType': meta['openaccessType'],
                    'openaccessUserLicense': meta['openaccessUserLicense'],
                    'data_availability_statement': das_text,
                    'data_links': '; '.join(links)
                })

            except Exception as e:
                msg = f"{doi}: ERROR → {e}"
                print(msg)
                log_error(log_file, msg)

    print(f"CSV saved → {csv_path}")
    print(f"Log saved → {log_file}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python client.py <input_docx>")
        sys.exit(1)
    main(sys.argv[1])
