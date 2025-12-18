#!/usr/bin/env python3
import sys, logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .doi_extract import extract_from_docx, extract_from_txt
from .crossref_client import fetch_crossref_json
from .elsevier_client import fetch_elsevier_xml
from .springer_client import fetch_springer_meta, fetch_springer_oa_json, fetch_springer_oa_xml
from .wiley_client import fetch_wiley_tdm_xml
from .resolver import try_doi_resolver_pdf, try_doi_resolver_xml
from .parser import parse_elsevier_xml_file
from .utils import output_subdir, write_csv
from . import OUTPUT_BASE

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("fair_champion")

MAX_WORKERS = 6

def setup_logging(input_filename):
    """Setup file logging with input filename in log name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"{timestamp}_{Path(input_filename).stem}_processing.log"
    log_path = OUTPUT_BASE / log_name
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.info(f"Processing started for: {input_filename}")
    return log_path

def process_single(entry, outdir: Path):
    title_in = entry.get("title", "")
    doi = entry.get("doi")
    row = {
        "title": title_in,
        "doi": doi or "",
        "publisher": "",
        "type": "",
        "authors": "",
        "journal": "",
        "openaccess": "",
        "data_availability_statement": "",
        "data_links": "",
        "xml_path": "",
        "pdf_path": "",
        "status": ""
    }

    if not doi:
        logger.warning(f"WARNING: No DOI found for: {title_in}")
        row["status"] = "no_doi"
        return row

    logger.info(f"Processing DOI: {doi} ...")

    # Crossref metadata
    logger.debug(f"  → Fetching Crossref metadata...")
    meta = fetch_crossref_json(doi)
    if meta:
        logger.debug(f"    ✓ Crossref metadata found")
        row["publisher"] = meta.get("publisher", "")
        row["type"] = meta.get("type", "")
        authors = []
        for a in meta.get("author", []):
            name = " ".join(filter(None, [a.get("given"), a.get("family")]))
            if name:
                authors.append(name)
        row["authors"] = ", ".join(authors)
        container = meta.get("container-title", [])
        row["journal"] = container[0] if container else ""
        links = meta.get("link", []) or []
        row["openaccess"] = any(link.get("content-version") == "vor" for link in links)
    else:
        logger.debug(f"    ✗ Crossref metadata not found")
        row["status"] = "crossref_missing"

    # Try DOI resolver XML
    logger.debug(f"  → Trying DOI resolver XML...")
    xml_text = try_doi_resolver_xml(doi)
    if xml_text:
        logger.debug(f"    ✓ DOI resolver XML found")
        xml_path = outdir / f"{doi.replace('/', '_')}.xml"
        xml_path.write_text(xml_text, encoding="utf-8")
        row["xml_path"] = str(xml_path)
        gen = parse_elsevier_xml_file(xml_path)
        if gen.get("title") and (not row["title"] or row["title"].strip() == ""):
            row["title"] = gen["title"]
        row["status"] = row["status"] or "xml_resolver"
    else:
        logger.debug(f"    ✗ DOI resolver XML not available")

    # Elsevier detailed XML (only if we don't have XML yet)
    if ((row["publisher"] and "elsevier" in row["publisher"].lower()) or doi.startswith("10.1016")) and not row["xml_path"]:
        logger.debug(f"  → Detected Elsevier publisher, fetching full XML...")
        elsev_xml = fetch_elsevier_xml(doi)
        if elsev_xml:
            logger.debug(f"    ✓ Elsevier XML fetched")
            xml_path = outdir / f"{doi.replace('/', '_')}_elsevier.xml"
            xml_path.write_text(elsev_xml, encoding="utf-8")
            row["xml_path"] = str(xml_path)
            parsed = parse_elsevier_xml_file(Path(xml_path))
            row.update(parsed)
            row["status"] = row["status"] or "elsevier_xml"
        else:
            logger.debug(f"    ✗ Elsevier XML not available")

    # Springer: meta + OA (check before Elsevier since .1186 is Springer-specific)
    if row["publisher"] and "springer" in row["publisher"].lower() or doi.startswith("10.1186"):
        logger.debug(f"  → Detected Springer/BMC publisher, fetching metadata...")
        spr = fetch_springer_meta(doi)
        if spr:
            logger.debug(f"    ✓ Springer metadata found")
            row["journal"] = spr.get("publicationName") or row["journal"]
            creators = spr.get("creators") or []
            if creators and not row["authors"]:
                authors = [c.get("creator") for c in creators if c.get("creator")]
                row["authors"] = ", ".join(authors)
            row["status"] = row["status"] or "springer_meta"
        else:
            logger.debug(f"    ✗ Springer metadata not found")
        
        # try OA XML
        logger.debug(f"  → Trying Springer OA XML...")
        spr_oa_xml = fetch_springer_oa_xml(doi)
        if spr_oa_xml:
            logger.debug(f"    ✓ Springer OA XML found")
            xml_path = outdir / f"{doi.replace('/', '_')}_springer.xml"
            xml_path.write_text(spr_oa_xml, encoding="utf-8")
            row["xml_path"] = str(xml_path)
            parsed = parse_elsevier_xml_file(xml_path)
            row.update(parsed)
            row["status"] = row["status"] or "springer_oa_xml"
        else:
            logger.debug(f"    ✗ Springer OA XML not available, trying OA JSON...")
            # try OA JSON to discover pdf link
            spr_oa = fetch_springer_oa_json(doi)
            if spr_oa:
                logger.debug(f"    ✓ Springer OA JSON found")
                urls = spr_oa.get("url", []) or []
                pdf_url = None
                for u in urls:
                    if isinstance(u, dict):
                        s = u.get("value") or ""
                    else:
                        s = u
                    if s.endswith('.pdf'):
                        pdf_url = s
                        break
                if pdf_url:
                    try:
                        logger.debug(f"    → Downloading PDF from {pdf_url[:50]}...")
                        import requests
                        r = requests.get(pdf_url, timeout=30)
                        if r.status_code == 200 and r.headers.get("Content-Type","").startswith("application/pdf"):
                            pdfp = outdir / f"{doi.replace('/', '_')}.pdf"
                            pdfp.write_bytes(r.content)
                            row["pdf_path"] = str(pdfp)
                            row["status"] = row["status"] or "springer_oa_pdf"
                            logger.debug(f"      ✓ PDF saved")
                        else:
                            logger.debug(f"      ✗ PDF download failed (status {r.status_code})")
                    except Exception as e:
                        logger.warning(f"      ✗ PDF download error: {e}")
                else:
                    logger.debug(f"    ✗ No PDF URL found in Springer OA JSON")
            else:
                logger.debug(f"    ✗ Springer OA JSON not available")

    # Wiley via TDM
    if row["publisher"] and "wiley" in row["publisher"].lower():
        logger.debug(f"  → Detected Wiley publisher, fetching TDM XML...")
        wxml = fetch_wiley_tdm_xml(doi)
        if wxml:
            logger.debug(f"    ✓ Wiley TDM XML fetched")
            xml_path = outdir / f"{doi.replace('/', '_')}_wiley.xml"
            xml_path.write_text(wxml, encoding="utf-8")
            row["xml_path"] = str(xml_path)
            parsed = parse_elsevier_xml_file(xml_path)
            row.update(parsed)
            row["status"] = row["status"] or "wiley_tdm"
        else:
            logger.debug(f"    ✗ Wiley TDM XML not available")

    # If still no PDF, try authenticated resolver then OA fallback
    if not row["pdf_path"]:
        logger.debug(f"  → Trying PDF resolver...")
        pdf_out = outdir / f"{doi.replace('/', '_')}.pdf"
        pdf_saved = try_doi_resolver_pdf(doi, pdf_out)
        if pdf_saved:
            logger.debug(f"    ✓ PDF saved via resolver")
            row["pdf_path"] = str(pdf_saved)
            row["status"] = row["status"] or "pdf_resolver"
        else:
            logger.debug(f"    ✗ PDF not available")

    # final fallback status
    if not row["status"]:
        row["status"] = "metadata_only"

    logger.info(f"  → Status: {row['status']}")
    return row

def process_docx(docx_path):
    entries = extract_from_docx(docx_path)
    outdir = output_subdir()
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_single, e, outdir): e for e in entries}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                results.append(r)
            except Exception as ex:
                logger.error(f"Task failed: {ex}")
    csv_path = outdir / f"{Path(docx_path).stem}_data.csv"
    write_csv(results, csv_path)
    logger.info(f"CSV saved → {csv_path}")
    logger.info(f"Files saved under → {outdir}")

def process_txt(txt_path):
    entries = extract_from_txt(txt_path)
    outdir = output_subdir()
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_single, e, outdir): e for e in entries}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                results.append(r)
            except Exception as ex:
                logger.error(f"Task failed: {ex}")
    csv_path = outdir / f"{Path(txt_path).stem}_data.csv"
    write_csv(results, csv_path)
    logger.info(f"CSV saved → {csv_path}")
    logger.info(f"Files saved under → {outdir}")

def main(argv):
    if len(argv) != 2:
        print("Usage: python -m code.fair_champion.get_data <input_file>\n"
              "   or: python -m code.fair_champion <input_file>\n"
              "Supported formats: .docx, .txt")
        return 1
    
    input_path = Path(argv[1])
    setup_logging(argv[1])
    
    if input_path.suffix.lower() == ".docx":
        process_docx(argv[1])
    elif input_path.suffix.lower() == ".txt":
        process_txt(argv[1])
    else:
        print(f"Error: Unsupported file format '{input_path.suffix}'. Use .docx or .txt")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
