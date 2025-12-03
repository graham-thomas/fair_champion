#!/usr/bin/env python3
"""
Unified XML-only DOI processing pipeline
- Reads a DOCX input with lines "<title> <doi>"
- Detects publisher via Crossref (JSON primary, XML fallback)
- Routes to publisher-specific XML fetchers (Elsevier, Springer)
- Saves XML files under analysis/els_client/YYYY_MM/
- Parses XML using your existing parsing functions (DAS, metadata)
- Writes a CSV and processing log

Usage:
    python code/els_router_xml_pipeline.py data/test-5.docx

Requirements:
    pip install python-dotenv requests python-docx
"""
import os
import re
import csv
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from docx import Document
import xml.etree.ElementTree as ET

# -------------------------
# Configuration
# -------------------------
load_dotenv(Path.home() / ".config/api_keys.env")
ELSEVIER_KEY = os.getenv("ELSEVIER_API_KEY")
SPRINGER_KEY = os.getenv("SPRINGER_API_KEY")  # optional

if not ELSEVIER_KEY:
    raise SystemExit("Error: ELSEVIER_API_KEY not found in ~/.config/api_keys.env")

CROSSREF_API = "https://api.crossref.org/works/"
SPRINGER_API = "https://api.springernature.com/metadata/xml"
ELSEVIER_ARTICLE_API = "https://api.elsevier.com/content/article/doi/"

DOI_REGEX = r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+"

# -------------------------
# Utilities: IO / Logging
# -------------------------
def log_error(logfile: Path, message: str):
    with open(logfile, "a", encoding="utf-8") as lf:
        lf.write(message.rstrip() + "\n")

def save_xml_text(doi: str, xml_text: str, output_dir: Path) -> Path:
    safe = doi.replace("/", "_")
    p = output_dir / f"{safe}.xml"
    with open(p, "w", encoding="utf-8") as f:
        f.write(xml_text)
    return p

# -------------------------
# Crossref: publisher detection (JSON primary, XML fallback)
# -------------------------
def get_publisher_from_crossref(doi: str) -> str:
    url = CROSSREF_API + requests.utils.requote_uri(doi)
    headers = {"Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json().get("message", {})
        return data.get("publisher", "Unknown")
    except Exception:
        # fallback to XML
        try:
            r = requests.get(url, headers={"Accept": "application/xml"}, timeout=15)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            pub = root.find('.//publisher')
            if pub is not None and pub.text:
                return pub.text
        except Exception:
            pass
    return "Unknown"

# -------------------------
# Elsevier XML fetcher
# -------------------------
def fetch_elsevier_xml(doi: str) -> str:
    url = ELSEVIER_ARTICLE_API + requests.utils.requote_uri(doi)
    headers = {"X-ELS-APIKey": ELSEVIER_KEY, "Accept": "application/xml"}
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.text

# -------------------------
# Springer XML fetcher
# -------------------------
def fetch_springer_xml(doi: str) -> str:
    if not SPRINGER_KEY:
        return {"error": "no_springer_key"}
    params = {"q": f"doi:{doi}", "api_key": SPRINGER_KEY}
    r = requests.get(SPRINGER_API, params=params, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.text

# -------------------------
# Parsing helpers
# -------------------------
def parse_das_from_xml(xml_path: Path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    ns = {k: v for k, v in [node for _, node in ET.iterparse(xml_path, events=['start-ns'])]}

    das_text = ""
    links = []

    candidates = [
        './/ce:data-availability',
        './/data-availability',
        './/availability',
        './/dataAvailability'
    ]

    for cand in candidates:
        da_section = root.find(cand, ns)
        if da_section is not None:
            paras = da_section.findall('.//ce:para', ns) or da_section.findall('.//para', ns) or da_section.findall('.//p', ns)
            if paras:
                das_text = ' '.join([''.join(p.itertext()).strip() for p in paras])
                for p in paras:
                    for link in p.findall('.//ce:inter-ref', ns) + p.findall('.//ext-link', ns) + p.findall('.//a', ns):
                        href = link.attrib.get('{http://www.w3.org/1999/xlink}href') or link.attrib.get('href')
                        if href:
                            links.append(href)
                break

    return das_text, links

def parse_article_metadata(xml_path: Path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    ns = {k: v for k, v in [node for _, node in ET.iterparse(xml_path, events=['start-ns'])]}

    journal = root.find('.//ce:publication-name', ns) or root.find('.//prism:publicationName', ns) or root.find('.//journal-title', ns)
    journal_text = journal.text.strip() if journal is not None and journal.text else ""

    authors_elems = root.findall('.//ce:author', ns) or root.findall('.//author', ns)
    authors = []
    corresponding_author = ""
    corresponding_email = ""

    for a in authors_elems:
        given = a.find('ce:given-name', ns) or a.find('given-name', ns)
        surname = a.find('ce:surname', ns) or a.find('surname', ns) or a.find('family-name', ns)
        name = ''
        if given is not None and given.text:
            name += given.text + ' '
        if surname is not None and surname.text:
            name += surname.text
        if name.strip():
            authors.append(name.strip())

        email_elem = a.find('.//ce:e-address', ns) or a.find('.//e-address', ns) or a.find('.//email', ns)
        if email_elem is not None and email_elem.text:
            corresponding_email = email_elem.text.strip()
            if not corresponding_author:
                corresponding_author = name.strip()

        cross_refs = a.findall('.//ce:cross-ref', ns) or a.findall('.//cross-ref', ns)
        for ref in cross_refs:
            if ref.attrib.get('refid', '').lower().startswith('cor'):
                corresponding_author = name.strip()

    authors_text = ', '.join(authors)

    oa_article = ''
    oa_type = ''
    oa_user_license = ''
    try:
        default_ns = 'http://www.elsevier.com/xml/svapi/article/dtd'
        ns_oa = {'def': default_ns}
        def get_text_oa(tag_name):
            elem = root.find(f'.//def:{tag_name}', ns_oa)
            return elem.text.strip() if elem is not None and elem.text else ''
        oa_article = get_text_oa('openaccessArticle')
        oa_type = get_text_oa('openaccessType')
        oa_user_license = get_text_oa('openaccessUserLicense')
    except Exception:
        pass

    return {
        'authors': authors_text,
        'journal': journal_text,
        'corresponding_author': corresponding_author,
        'corresponding_email': corresponding_email,
        'openaccessArticle': oa_article,
        'openaccessType': oa_type,
        'openaccessUserLicense': oa_user_license
    }

# -------------------------
# High-level fetch + route
# -------------------------
def fetch_and_save_for_doi(doi: str, output_dir: Path, log_file: Path):
    publisher = get_publisher_from_crossref(doi)
    publisher_l = publisher.lower() if publisher else 'unknown'

    xml_text = None
    status = 'unsupported_publisher'

    if 'elsevier' in publisher_l or 'cell press' in publisher_l or 'lancet' in publisher_l:
        xml_text = fetch_elsevier_xml(doi)
        if xml_text is None:
            status = 'not_elsevier'
        else:
            status = 'elsevier'

    elif 'springer' in publisher_l or 'biomed central' in publisher_l:
        springer_res = fetch_springer_xml(doi)
        if isinstance(springer_res, dict) and springer_res.get('error'):
            log_error(log_file, f"{doi}: Springer API key missing; cannot fetch.")
            status = 'springer_key_missing'
        elif springer_res is None:
            status = 'not_found_springer'
        else:
            xml_text = springer_res
            status = 'springer'

    elif 'wiley' in publisher_l:
        log_error(log_file, f"{doi}: Wiley publisher detected — no XML client implemented; skipping.")
        status = 'wiley_unsupported'

    else:
        status = 'unsupported_publisher'

    if xml_text:
        xml_path = save_xml_text(doi, xml_text, output_dir)
        das_text, links = parse_das_from_xml(xml_path)
        meta = parse_article_metadata(xml_path)

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

        return {
            'doi': doi,
            'publisher': publisher,
            'status': status,
            'meta': meta,
            'data_availability_statement': das_text,
            'data_links': links,
            'xml_path': str(xml_path)
        }

    else:
        return {
            'doi': doi,
            'publisher': publisher,
            'status': status,
            'meta': None,
            'data_availability_statement': '',
            'data_links': [],
            'xml_path': ''
        }

# -------------------------
# DOCX reader
# -------------------------
def read_papers_from_docx(docx_file: str):
    doc = Document(docx_file)
    papers = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue
        dois = re.findall(DOI_REGEX, text)
        title = re.sub(DOI_REGEX, '', text).strip()
        if not dois:
            continue
        for doi in dois:
            papers.append({'title': title, 'doi': doi})
    return papers

# -------------------------
# Main runner
# -------------------------
def main(input_docx: str):
    papers = read_papers_from_docx(input_docx)

    now = datetime.now()
    folder_name = f"{now.year}_{now.month:02d}"
    output_dir = Path("analysis/els_router") / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = now.strftime("%Y%m%d_%H%M%S")
    log_file = output_dir / f"{timestamp}_processing.log"

    csv_path = output_dir / (Path(input_docx).stem + "_data.csv")

    fieldnames = [
        'title', 'doi', 'authors', 'journal',
        'corresponding_author', 'corresponding_email',
        'openaccessArticle', 'openaccessType',
        'openaccessUserLicense',
        'data_availability_statement', 'data_links', 'xml_path', 'status', 'publisher'
    ]

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for paper in papers:
            doi = paper['doi']
            title = paper['title']
            print(f"Processing DOI: {doi} ...")
            try:
                res = fetch_and_save_for_doi(doi, output_dir, log_file)
                row = {
                    'title': title,
                    'doi': doi,
                    'authors': res['meta']['authors'] if res['meta'] else '',
                    'journal': res['meta']['journal'] if res['meta'] else '',
                    'corresponding_author': res['meta']['corresponding_author'] if res['meta'] else '',
                    'corresponding_email': res['meta']['corresponding_email'] if res['meta'] else '',
                    'openaccessArticle': res['meta']['openaccessArticle'] if res['meta'] else '',
                    'openaccessType': res['meta']['openaccessType'] if res['meta'] else '',
                    'openaccessUserLicense': res['meta']['openaccessUserLicense'] if res['meta'] else '',
                    'data_availability_statement': res['data_availability_statement'],
                    'data_links': '; '.join(res['data_links']),
                    'xml_path': res['xml_path'],
                    'status': res['status'],
                    'publisher': res['publisher']
                }
                writer.writerow(row)

            except Exception as e:
                msg = f"{doi}: ERROR → {e}"
                print(msg)
                log_error(log_file, msg)

    print(f"CSV saved → {csv_path}")
    print(f"Log saved → {log_file}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python code/els_router_xml_pipeline.py <input_docx>")
        raise SystemExit(1)
    main(sys.argv[1])
