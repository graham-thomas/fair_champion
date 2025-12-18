"""
fair_scoring.py

Score papers against practical FAIR indicators
based on extracted data availability information.
"""

from typing import Iterable


# Known open, non-proprietary formats
OPEN_FORMATS = {
    "csv", "tsv", "txt",
    "fasta", "fastq", "fq",
    "bam", "sam",
    "gff", "gff3", "bed",
    "vcf",
    "json", "xml",
    "xlsx",  # borderline but widely used
}


# Known open licenses for data reuse
OPEN_LICENSES = {
    "cc0",
    "cc-by",
    "cc by",
    "cc-by-4.0",
    "cc by 4.0",
    "creative commons attribution",
    "creative commons zero",
    "open data commons",
    "odc-by",
    "odbl",
}


def score_fair(
    data_dois: Iterable[str] | None,
    formats: Iterable[str] | None,
    licenses: Iterable[str] | None,
) -> int:
    """
    Score FAIRness on a 0–4 scale.

    Returns:
        int: FAIR score
    """

    score = 0

    # ----------------------------
    # F: Findable
    # ----------------------------
    if data_dois:
        score += 1

    # ----------------------------
    # A: Accessible
    # ----------------------------
    if data_dois:
        score += 1

    # ----------------------------
    # I: Interoperable
    # ----------------------------
    if formats:
        for fmt in formats:
            if fmt.lower().lstrip(".") in OPEN_FORMATS:
                score += 1
                break

    # ----------------------------
    # R: Reusable
    # ----------------------------
    if licenses:
        for lic in licenses:
            lic_norm = lic.lower()
            if any(k in lic_norm for k in OPEN_LICENSES):
                score += 1
                break

    return score
