import re
from typing import List, Optional

doi_regex = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")

def extract_dois(text: str) -> List[str]:
    return doi_regex.findall(text)