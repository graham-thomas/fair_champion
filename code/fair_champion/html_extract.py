"""
html_extract.py

Extract FAIR-relevant information from article HTML.
"""

import re
from bs4 import BeautifulSoup


# ----------------------------
# Data Availability Statement
# ----------------------------

def extract_data_availability_statement(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # Common heading patterns
    for heading in soup.find_all(["h2", "h3", "h4"]):
        if heading.get_text(strip=True).lower().startswith("data availability"):
            # Collect text until next heading
            texts = []
            for sib in heading.find_next_siblings():
                if sib.name in ["h2", "h3", "h4"]:
                    break
                texts.append(sib.get_text(" ", strip=True))
            return " ".join(texts) if texts else None

    return None


# ----------------------------
# Dataset DOIs
# ----------------------------

DOI_REGEX = re.compile(r"\b10\.\d{4,9}/\S+\b", re.IGNORECASE)


def extract_dataset_dois(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    dois = set()
    for match in DOI_REGEX.findall(text):
        dois.add(match.rstrip(".,;)"))

    return sorted(dois)


# ----------------------------
# Data formats
# ----------------------------

FORMAT_REGEX = re.compile(
    r"\.(csv|tsv|txt|fasta|fastq|fq|bam|sam|gff3?|bed|vcf|json|xml|xlsx)\b",
    re.IGNORECASE,
)


def extract_data_formats(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    formats = set(fmt.lower() for fmt in FORMAT_REGEX.findall(text))
    return sorted(formats)


# ----------------------------
# Data licenses
# ----------------------------

LICENSE_PATTERNS = [
    r"cc[-\s]?by",
    r"cc[-\s]?0",
    r"creative commons",
    r"open data commons",
    r"odc[-\s]?by",
    r"odbl",
]


def extract_data_licenses(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()

    licenses = set()
    for pat in LICENSE_PATTERNS:
        if re.search(pat, text):
            licenses.add(pat.replace(r"\s?", " ").upper())

    return sorted(licenses)
