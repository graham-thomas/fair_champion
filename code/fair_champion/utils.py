from pathlib import Path
from datetime import datetime
from . import OUTPUT_BASE
import csv

def output_subdir():
    now = datetime.now()
    folder = OUTPUT_BASE / f"{now.year}_{now.month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder

def write_csv(rows, csv_path: Path):
    fieldnames = [
        "title", "doi", "publisher", "type", "authors", "journal",
        "openaccess", "data_availability_statement", "data_links",
        "xml_path", "pdf_path", "status"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
