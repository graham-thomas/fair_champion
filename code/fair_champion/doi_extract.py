import re
from docx import Document

DOI_REGEX = r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+"

def extract_from_docx(docx_path):
    docs = Document(docx_path)
    results = []
    for p in docs.paragraphs:
        text = p.text.strip()
        if not text:
            continue
        m = re.search(DOI_REGEX, text)
        doi = m.group(0) if m else None
        title = text.replace(doi, "").strip() if doi else text
        results.append({"title": title, "doi": doi})
    return results

def extract_from_txt(txt_path):
    """Extract DOIs and titles from a plain text file.
    
    Expected format: one entry per line, DOI can be anywhere in the line.
    The remainder of the line (after DOI is removed) becomes the title.
    If no DOI is found, the entire line is treated as title with no DOI.
    """
    results = []
    with open(txt_path, "r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            m = re.search(DOI_REGEX, text)
            doi = m.group(0) if m else None
            title = text.replace(doi, "").strip() if doi else text
            results.append({"title": title, "doi": doi})
    return results
