#!/usr/bin/env python3
"""
fair_champion.py

Usage:
    python code/fair_champion.py data/publication_list.docx
"""

from dotenv import load_dotenv
import os, re, sys, time, html, unicodedata, requests, pandas as pd, csv
from datetime import datetime
from bs4 import BeautifulSoup
from docx import Document

load_dotenv("~/.config/api_keys.env")  # or ".env"
elsevier_key = os.getenv("ELSEVIER_API_KEY")

# ---------- Utility ----------
def clean_text(s):
    if not s:
        return ""
    s = html.unescape(s)
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[\x00-\x1f\x7f-\x9f]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- DOI extraction ----------
def extract_dois_from_docx(path):
    text = "\n".join(p.text for p in Document(path).paragraphs)
    dois = re.findall(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', text, re.I)
    return sorted(set(dois))


# ---------- Europe PMC metadata ----------
def get_epmc_metadata(doi):
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{doi}&resultType=core&format=json"
    try:
        r = requests.get(url, timeout=20)
        r.encoding = r.apparent_encoding
        # Safely navigate the JSON structure
        results = r.json().get("resultList", {}).get("result", [])
        if not results:
            return "", "", "", None, None
        data = results[0]
        title   = clean_text(data.get("title", ""))
        authors = clean_text(data.get("authorString", ""))
        journal = clean_text(data.get("journalTitle", ""))
        # extract isOpenAccess and hasData if present (API returns "Y" or "N")
        is_open_access = data.get("isOpenAccess", None)
        has_data = data.get("hasData", None)
        # Normalize to True/False/None based on "Y"/"N" values
        if is_open_access is not None:
            is_open_access = (str(is_open_access).upper() == "Y")
        if has_data is not None:
            has_data = (str(has_data).upper() == "Y")
        return title, authors, journal, is_open_access, has_data
    except Exception:
        return "", "", "", None, None
# ---------- Crossref fallback ----------
def get_crossref_metadata(doi):
    url = f"https://api.crossref.org/works/{doi}"
    headers = {"Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.encoding = r.apparent_encoding
        msg = r.json()["message"]
        title = clean_text(" ".join(msg.get("title", [])))
        journal = clean_text(" ".join(msg.get("container-title", [])))
        authors = []
        for a in msg.get("author", []):
            parts = [a.get("given"), a.get("family")]
            name = " ".join(p for p in parts if p)
            if name:
                authors.append(name)
        return title, ", ".join(authors), journal
    except Exception:
        return "", "", ""


# ---------- Metadata from HTML <meta> ----------
def get_meta_tags(doi):
    url = f"https://doi.org/{doi}"
    headers = {"User-Agent": "Mozilla/5.0 (FAIR/1.0; +https://www.exeter.ac.uk/)"}
    try:
        r = requests.get(url, headers=headers, timeout=25, allow_redirects=True)
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        title_tag = soup.find("meta", {"name": "citation_title"})
        authors_tag = soup.find_all("meta", {"name": "citation_author"})
        journal_tag = soup.find("meta", {"name": "citation_journal_title"})
        title = clean_text(title_tag["content"]) if title_tag else ""
        authors = ", ".join(clean_text(a["content"]) for a in authors_tag if a.has_attr("content"))
        journal = clean_text(journal_tag["content"]) if journal_tag else ""
        return title, authors, journal
    except Exception:
        return "", "", ""


# ---------- Data availability extraction ----------
def fetch_data_availability(doi):
    url = f"https://doi.org/{doi}"
    headers = {"User-Agent": "Mozilla/5.0 (FAIR/1.0; +https://www.exeter.ac.uk/)"}
    try:
        r = requests.get(url, headers=headers, timeout=25, allow_redirects=True)
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        # Try to find a heading that indicates a Data Availability section and capture its content
        for h in soup.find_all(re.compile('^h[1-6]$')):
            if re.search(r'data (availability|accessibility)|availability of data|data sharing statement', h.get_text(), re.I):
                section = clean_text(h.get_text())
                fragments = []
                # Collect text from subsequent siblings until the next heading
                for sib in h.next_siblings:
                    if getattr(sib, 'name', None) and re.match('^h[1-6]$', sib.name):
                        break
                    if isinstance(sib, str):
                        fragments.append(sib.strip())
                    else:
                        fragments.append(sib.get_text(" ", strip=True))
                statement = clean_text(" ".join(fragments))
                if statement:
                    return section, statement

        # Fallback: search the full text for common patterns
        text = soup.get_text(" ", strip=True)
        patterns = [
            r"(Data (availability|accessibility)[^:]*[:.\n]\s*.{0,1000})",
            r"(Availability of data[^:]*[:.\n]\s*.{0,1000})",
            r"(Data sharing statement[^:]*[:.\n]\s*.{0,1000})"
        ]
        for p in patterns:
            m = re.search(p, text, re.I)
            if m:
                return "", clean_text(m.group(0))

        meta = soup.find("meta", {"name": "citation_data_availability"})
        if meta and meta.has_attr("content"):
            return "", clean_text(meta["content"])
        return "", ""
    except Exception:
        return "", ""


# ---------- FAIR scoring ----------
def score_fairness(statement, paper_doi):
    """Score 0-4: detect dataset DOI (not the paper DOI), repository, file formats, and license/open access.

    Inputs:
      - statement: the data availability statement text
      - paper_doi: the DOI of the paper (we ignore matches to this DOI)
    """
    if not statement:
        return 0
    stmt = statement.replace(paper_doi or "", "")
    score = 0

    # 1) Dataset DOI distinct from paper DOI
    dois = re.findall(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', stmt, re.I)
    dataset_dois = [d for d in dois if d.lower() != (paper_doi or "").lower()]
    if dataset_dois:
        score += 1

    # 2) Repository mention
    if re.search(r'ENA|European Nucleotide Archive|GEO|Gene Expression Omnibus|Figshare|Zenodo|Dryad|ArrayExpress|EBI|GenBank|PRIDE|PDB|OSF|Dataverse|Mendeley Data|MG-RAST', stmt, re.I):
        score += 1

    # 3) File formats / accessible raw data mention
    if re.search(r'FASTA|CSV|JSON|TSV|TXT|HDF5|BAM|VCF|XML|NetCDF|raw data|supplementary data', stmt, re.I):
        score += 1

    # 4) License / open reuse statement
    if re.search(r'CC[- ]?(BY|0)|Creative Commons|open license|MIT license|public domain|available under', stmt, re.I):
        score += 1

    return score


# ---------- Main ----------
def process_publication_list(input_path, output_dir="analysis"):
    if not os.path.exists(input_path):
        sys.exit(f"❌ Input not found: {input_path}")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(input_path))[0]
    output_csv = os.path.join(output_dir, f"{timestamp}_{base}.csv")

    dois = extract_dois_from_docx(input_path)
    print(f"📰 Found {len(dois)} DOIs")

    records = []
    for doi in dois:
        # Query multiple sources and prefer the first non-empty value for each field.
        epmc_title, epmc_authors, epmc_journal, epmc_is_open, epmc_has_data = get_epmc_metadata(doi)
        cr_title, cr_authors, cr_journal = get_crossref_metadata(doi)
        meta_title, meta_authors, meta_journal = get_meta_tags(doi)

        # Prefer EPMC, then Crossref, then HTML meta tags for each field
        title = epmc_title or cr_title or meta_title
        authors = epmc_authors or cr_authors or meta_authors
        journal = epmc_journal or cr_journal or meta_journal
        section, statement = fetch_data_availability(doi)
        fair = score_fairness(statement, doi)
        records.append({
            "Paper_DOI": doi,
            "Title": title,
            "Authors": authors,
            "Journal": journal,
            "IsOpenAccess": epmc_is_open,
            "HasData": epmc_has_data,
            "Data_Availability_Section": section,
            "Data_Availability_Statement": statement,
            "FAIR_Score": fair
        })
        time.sleep(2)

    # Ensure column order: Paper_DOI, Title, Authors, Journal, IsOpenAccess, HasData,
    # Data_Availability_Section, Data_Availability_Statement, FAIR_Score
    cols = [
        "Paper_DOI", "Title", "Authors", "Journal", "IsOpenAccess", "HasData",
        "Data_Availability_Section", "Data_Availability_Statement", "FAIR_Score"
    ]
    df = pd.DataFrame(records)
    df = df.reindex(columns=[c for c in cols if c in df.columns])
# Write UTF-8 with BOM to help Excel recognise UTF-8 and avoid garbled characters
    df.to_csv(output_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    print(f"💾 Results written to {output_csv}")

    best = df.sort_values("FAIR_Score", ascending=False).head(1)
    if not best.empty:
        c = best.iloc[0]
        print(
            f"\n🏆 FAIR Champion\n"
            f"Title: {c.Title}\n"
            f"Authors: {c.Authors}\n"
            f"Journal: {c.Journal}\n"
            f"DOI: {c.Paper_DOI}\n"
            f"Is Open Access: {c.IsOpenAccess}\n"
            f"Has Data: {c.HasData}\n"
            f"FAIR Score: {c.FAIR_Score}/4\n"
        )
    else:
        print("No FAIR data availability statements found.")


# ---------- CLI ----------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python code/fair_champion.py data/publication_list.docx")
        sys.exit(1)
    process_publication_list(sys.argv[1])
