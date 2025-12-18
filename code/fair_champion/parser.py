import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List

def _collect_namespaces(xml_path: Path) -> Dict[str,str]:
    ns_map = {}
    for event, elem in ET.iterparse(str(xml_path), events=("start-ns",)):
        prefix, uri = elem
        ns_map[prefix] = uri
    mapping = {}
    if None in ns_map:
        mapping["els"] = ns_map[None]
    elif "" in ns_map:
        mapping["els"] = ns_map[""]
    if "dc" in ns_map:
        mapping["dc"] = ns_map["dc"]
    if "prism" in ns_map:
        mapping["prism"] = ns_map["prism"]
    if "ce" in ns_map:
        mapping["ce"] = ns_map["ce"]
    if "xlink" in ns_map:
        mapping["xlink"] = ns_map["xlink"]
    for p, uri in ns_map.items():
        if p is None:
            continue
        if p not in mapping:
            mapping[p] = uri
    return mapping

def _get_text(el):
    if el is None:
        return ""
    return "".join(el.itertext()).strip()

def _find_by_localname(root, local):
    for el in root.iter():
        tag = el.tag
        if isinstance(tag, str):
            if tag == local or tag.endswith("}" + local):
                return el
    return None

def parse_elsevier_xml_file(xml_path: Path) -> Dict[str, object]:
    """
    Focused Elsevier XML parser:
      - title, doi (prism preferred), journal
      - authors (coredata dc:creator OR author-group/author only)
      - openaccess
      - data_availability_statement and data_links
    """
    ns = _collect_namespaces(xml_path)
    tree = ET.parse(str(xml_path))
    root = tree.getroot()

    # Title
    title_el = None
    if "dc" in ns:
        title_el = root.find(".//dc:title", ns)
    if title_el is None:
        title_el = _find_by_localname(root, "title")
    title = _get_text(title_el) if title_el is not None else ""

    # DOI: prefer prism:doi, else dc:identifier that contains a DOI
    doi = ""
    if "prism" in ns:
        el = root.find(".//prism:doi", ns)
        if el is not None and _get_text(el):
            doi = _get_text(el).strip()
    if not doi:
        if "dc" in ns:
            # find all dc:identifier and pick the one that looks like a DOI
            for el in root.findall(".//dc:identifier", ns):
                txt = _get_text(el)
                if not txt:
                    continue
                # sometimes Crossref/Elsevier store "doi:10.xxxx" or "10.xxxx"
                candidate = txt.strip()
                if candidate.startswith("doi:"):
                    candidate = candidate[len("doi:"):]
                if candidate.startswith("10."):
                    doi = candidate
                    break
        # fallback local-name search
    if not doi:
        el = _find_by_localname(root, "doi") or _find_by_localname(root, "identifier")
        if el is not None:
            txt = _get_text(el)
            if txt:
                candidate = txt.strip()
                if candidate.startswith("doi:"):
                    candidate = candidate[len("doi:"):]
                if candidate.startswith("10."):
                    doi = candidate

    # Journal: prism:publicationName or coredata publication-name
    journal = ""
    if "prism" in ns:
        el = root.find(".//prism:publicationName", ns)
        if el is not None and _get_text(el):
            journal = _get_text(el)
    if not journal:
        el = root.find(".//{http://www.elsevier.com/xml/svapi/article/dtd}publication-name")
        if el is not None and _get_text(el):
            journal = _get_text(el)
    if not journal:
        el = _find_by_localname(root, "publicationName") or _find_by_localname(root, "publication-name")
        if el is not None:
            journal = _get_text(el)

    # Authors: prefer coredata dc:creator elements (clean), else author-group/author direct children only
    authors_list: List[str] = []
    # 1) coredata dc:creator
    if "dc" in ns:
        for el in root.findall(".//dc:creator", ns):
            txt = _get_text(el)
            if txt:
                authors_list.append(txt)
    # 2) structured authors under author-group/author
    # find all author-group elements, then take only direct 'author' children
    for ag in root.findall(".//author-group") + root.findall(".//{http://www.elsevier.com/xml/svapi/article/dtd}author-group"):
        for author in [c for c in ag if (isinstance(c.tag, str) and (c.tag.endswith("}author") or c.tag == "author"))]:
            # try given-name + surname
            given = None
            surname = None
            # check namespaced tags first
            # try several common names for given/surname
            for gtag in ("given-name", "givenName", "given"):
                g = author.find(f".//{gtag}") or author.find(f".//{{http://www.elsevier.com/xml/svapi/article/dtd}}{gtag}")
                if g is not None and _get_text(g):
                    given = _get_text(g)
                    break
            for stag in ("surname", "family-name", "familyName", "surname"):
                s = author.find(f".//{stag}") or author.find(f".//{{http://www.elsevier.com/xml/svapi/article/dtd}}{stag}")
                if s is not None and _get_text(s):
                    surname = _get_text(s)
                    break
            if given and surname:
                name = f"{given} {surname}".strip()
                if name and name not in authors_list:
                    authors_list.append(name)
            else:
                # fallback: text of author element (strip roles if present)
                txt = _get_text(author)
                if txt:
                    # remove common role strings that sometimes sit inside author nodes
                    # (this is a heuristic)
                    cleaned = " ".join([line.strip() for line in txt.splitlines() if line.strip() and not line.strip().endswith("writing") and len(line.strip()) < 120])
                    if cleaned and cleaned not in authors_list:
                        authors_list.append(cleaned)

    authors = ", ".join(authors_list)

    # Open access: look for element named openaccess or openaccessArticle under coredata or anywhere
    oa_val = ""
    # search coredata children
    coredata_el = _find_by_localname(root, "coredata")
    if coredata_el is not None:
        for child in coredata_el:
            tag = child.tag
            if isinstance(tag, str) and (tag.endswith("}openaccess") or tag.endswith("}openaccessArticle") or tag == "openaccess" or tag == "openaccessArticle"):
                va = _get_text(child)
                if va:
                    oa_val = va.strip()
                    break
    # fallback global search
    if not oa_val:
        el = _find_by_localname(root, "openaccess") or _find_by_localname(root, "openaccessArticle")
        if el is not None:
            oa_val = _get_text(el).strip()

    # Data availability
    das_text = ""
    das_links: List[str] = []
    das_nodes = []
    for el in root.iter():
        if isinstance(el.tag, str) and (el.tag.endswith("}data-availability") or el.tag == "data-availability" or el.tag.endswith("}dataAvailability") or el.tag == "dataAvailability"):
            das_nodes.append(el)
    if das_nodes:
        parts = []
        for das in das_nodes:
            # collect paragraphs (ce:para or p)
            for para in list(das):
                if not isinstance(para.tag, str):
                    continue
                if para.tag.endswith("}para") or para.tag == "para" or para.tag.endswith("}p") or para.tag == "p":
                    t = _get_text(para)
                    if t:
                        parts.append(t)
                    # look for inter-ref or ext-link inside this para
                    for child in para.iter():
                        if not isinstance(child.tag, str):
                            continue
                        if child.tag.endswith("}inter-ref") or child.tag == "inter-ref":
                            href = child.attrib.get("{http://www.w3.org/1999/xlink}href") or child.attrib.get("href")
                            if href and href not in das_links:
                                das_links.append(href)
                        if child.tag.endswith("}ext-link") or child.tag == "ext-link" or child.tag.endswith("}a") or child.tag == "a":
                            href = child.attrib.get("href") or child.attrib.get("{http://www.w3.org/1999/xlink}href")
                            if href and href not in das_links:
                                das_links.append(href)
            # also check for any inter-ref directly under das
            for child in das.iter():
                if not isinstance(child.tag, str):
                    continue
                if child.tag.endswith("}inter-ref") or child.tag == "inter-ref":
                    href = child.attrib.get("{http://www.w3.org/1999/xlink}href") or child.attrib.get("href")
                    if href and href not in das_links:
                        das_links.append(href)
        das_text = " ".join(parts).strip()

    # Convert OA numeric/string to TRUE/FALSE for CSV output
    oa_bool = ""
    if oa_val and isinstance(oa_val, str):
        if oa_val.strip() == "1":
            oa_bool = "TRUE"
        elif oa_val.strip() == "0":
            oa_bool = "FALSE"

    return {
        "title": title,
        "doi": doi,
        "journal": journal,
        "authors": authors,
        "openaccess": oa_bool,
        "data_availability_statement": das_text,
        "data_links": das_links
    }
