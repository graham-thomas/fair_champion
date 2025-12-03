#!/usr/bin/env python3
"""
data_fair_assessment.py

Assess data repositories linked in papers against FAIR principles.
Extracts information about datasets (DOI, format, license, etc.) from data links
and scores them for FAIRness.

Usage:
    python code/data_fair_assessment.py analysis/next_latest/2025_11/test-1_data.csv
"""

import csv
import logging
import re
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set

import requests
from bs4 import BeautifulSoup


# ---------- Constants ----------
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 2  # seconds between requests
MAX_FILES_TO_DISPLAY = 10
MAX_FORMATS_TO_DISPLAY = 10

USER_AGENT = 'Mozilla/5.0 (FAIR Assessment Bot/1.0; +https://www.exeter.ac.uk/)'

FILE_EXTENSIONS = ['csv', 'tsv', 'txt', 'json', 'xml', 'fasta', 'fastq', 
                   'bam', 'vcf', 'hdf5', 'xlsx', 'xls', 'pdf', 'zip', 
                   'tar.gz', 'tgz']

REPOSITORY_PATTERNS = {
    'Mendeley Data': r'mendeley\.com|data\.mendeley',
    'Zenodo': r'zenodo\.org',
    'Figshare': r'figshare\.com',
    'Dryad': r'datadryad\.org',
    'GitHub': r'github\.com',
    'ENA': r'ebi\.ac\.uk/ena|www\.ebi\.ac\.uk/ena',
    'GenBank': r'ncbi\.nlm\.nih\.gov/genbank',
    'GEO': r'ncbi\.nlm\.nih\.gov/geo',
    'ArrayExpress': r'ebi\.ac\.uk/arrayexpress',
    'PRIDE': r'ebi\.ac\.uk/pride',
    'OSF': r'osf\.io'
}

LICENSE_PATTERNS = [
    r'CC[- ]?BY[- ]?(?:NC)?[- ]?(?:SA)?[- ]?(?:\d\.\d)?',
    r'Creative Commons',
    r'CC0',
    r'MIT License',
    r'GPL',
    r'Apache License',
    r'Public Domain',
    r'Open Database License'
]

# Logging will be configured in main() with a timestamped log file
logger = logging.getLogger(__name__)


# ---------- Utility Functions ----------
def clean_text(text: str) -> str:
    """Clean and normalize whitespace in text."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def is_metadata_file(filename: str) -> bool:
    """Check if filename is a metadata file (LICENSE, README, etc.)."""
    return bool(re.match(r'^(license|readme|citation)', filename, re.I))


# ---------- Repository-specific extractors ----------
def extract_mendeley_files(url: str, headers: Dict[str, str]) -> List[str]:
    """Extract file list from Mendeley Data using API."""
    dataset_id_match = re.search(r'/datasets/([a-z0-9]+)', url)
    if not dataset_id_match:
        return []
    
    dataset_id = dataset_id_match.group(1)
    files_url = f'https://data.mendeley.com/api/datasets/{dataset_id}/files'
    
    try:
        response = requests.get(files_url, headers=headers, timeout=20)
        if response.status_code == 200:
            files_data = response.json()
            if isinstance(files_data, list):
                return [
                    f.get('filename', '') 
                    for f in files_data 
                    if f.get('filename') and not is_metadata_file(f.get('filename', ''))
                ]
    except Exception as e:
        logger.debug(f"Mendeley API error: {e}")
    
    return []


def extract_filenames_from_html(html_content: str) -> Set[str]:
    """Extract data filenames from HTML content using regex patterns."""
    ext_pattern = '|'.join(FILE_EXTENSIONS)
    filename_patterns = [
        rf'([a-zA-Z0-9_\-]+\.(?:{ext_pattern}))',
        rf'filename["\']?\s*[:=]\s*["\']?([^"\'>\\s]+\.(?:{ext_pattern}))["\']?'
    ]
    
    filenames = set()
    for pattern in filename_patterns:
        matches = re.findall(pattern, html_content, re.I)
        for match in matches:
            filename = match[0] if isinstance(match, tuple) else match
            if not is_metadata_file(filename):
                filenames.add(filename)
    
    return filenames


def extract_formats_from_html(html_content: str, soup: BeautifulSoup) -> Set[str]:
    """Extract file formats from HTML content and meta tags."""
    ext_pattern = '|'.join(FILE_EXTENSIONS)
    format_patterns = [
        rf'(?:file|download|dataset|data)\s+[^<>]{{0,50}}\.(?:{ext_pattern})\b',
        rf'\.(?:{ext_pattern})\s+(?:file|download|format)',
        rf'href=["\'][^"\']*\.(?:{ext_pattern})["\']',
        rf'data-[^=]*=["\'][^"\']*\.(?:{ext_pattern})["\']'
    ]
    
    formats = set()
    for pattern in format_patterns:
        matches = re.findall(pattern, html_content, re.I)
        for match in matches:
            if isinstance(match, tuple):
                formats.update(m.upper().lstrip('.') for m in match if m)
            else:
                formats.add(match.upper().lstrip('.'))
    
    # Check meta tags
    for meta in soup.find_all('meta'):
        content = meta.get('content', '')
        if content:
            for ext in FILE_EXTENSIONS:
                if f'.{ext}' in content.lower():
                    formats.add(ext.upper())
    
    return formats


def identify_repository(url: str) -> str:
    """Identify which repository the URL belongs to."""
    for repo_name, pattern in REPOSITORY_PATTERNS.items():
        if re.search(pattern, url, re.I):
            return repo_name
    return ''


def extract_license(page_text: str) -> str:
    """Extract license information from page text."""
    for pattern in LICENSE_PATTERNS:
        match = re.search(pattern, page_text, re.I)
        if match:
            return clean_text(match.group(0))
    return ''


def extract_dataset_doi(page_text: str, paper_doi: str) -> str:
    """Extract dataset DOI (distinct from paper DOI) from page text."""
    dois = re.findall(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', page_text, re.I)
    dataset_dois = [d for d in dois if d.lower() != paper_doi.lower()]
    return dataset_dois[0] if dataset_dois else ''


def query_datacite_api(dataset_doi: str, headers: Dict[str, str]) -> str:
    """Query DataCite API for file information."""
    try:
        datacite_url = f"https://api.datacite.org/dois/{dataset_doi}"
        response = requests.get(datacite_url, headers=headers, timeout=20)
        if response.status_code == 200:
            data_attr = response.json().get('data', {}).get('attributes', {})
            content_urls = data_attr.get('contentUrl', [])
            
            if isinstance(content_urls, list):
                ext_pattern = '|'.join(FILE_EXTENSIONS)
                for file_url in content_urls:
                    match = re.search(rf'/([^/]+\.(?:{ext_pattern}))$', file_url, re.I)
                    if match:
                        return match.group(1)
    except Exception as e:
        logger.debug(f"DataCite API error: {e}")
    
    return ''


# ---------- Main assessment function ----------
def assess_dataset_url(url: str, paper_doi: str) -> Dict[str, any]:
    """
    Visit a dataset URL and extract FAIR-relevant information.
    
    Args:
        url: URL of the dataset repository
        paper_doi: DOI of the associated paper
    
    Returns:
        Dictionary containing:
        - dataset_doi: DOI of the dataset
        - file_name: Comma-separated list of data files
        - file_formats: Comma-separated list of file formats
        - license: License information
        - repository: Repository name
        - accessibility: Accessibility status
        - fair_score: FAIR score (0-4)
    """
    result = {
        'url': url,
        'dataset_doi': '',
        'file_name': '',
        'file_formats': '',
        'license': '',
        'repository': '',
        'accessibility': 'Unknown',
        'fair_score': 0
    }
    
    try:
        headers = {'User-Agent': USER_AGENT}
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text(' ', strip=True)
        html_content = response.text
        
        # 1. Identify repository first (needed for repository-specific logic)
        result['repository'] = identify_repository(url)
        if result['repository']:
            result['fair_score'] += 1
        
        # 2. Extract file names using repository-specific methods
        filenames = set()
        
        # Try repository-specific API
        if result['repository'] == 'Mendeley Data':
            mendeley_files = extract_mendeley_files(url, headers)
            if mendeley_files:
                filenames.update(mendeley_files)
        
        # Fall back to HTML parsing
        if not filenames:
            filenames = extract_filenames_from_html(html_content)
        
        if filenames:
            result['file_name'] = ', '.join(sorted(filenames)[:MAX_FILES_TO_DISPLAY])
        
        # 3. Extract dataset DOI
        dataset_doi = extract_dataset_doi(page_text, paper_doi)
        if dataset_doi:
            result['dataset_doi'] = dataset_doi
            result['fair_score'] += 1
            
            # Try DataCite API for additional file information
            if not result['file_name']:
                datacite_filename = query_datacite_api(dataset_doi, headers)
                if datacite_filename:
                    result['file_name'] = datacite_filename
        
        # 4. Extract file formats
        formats = extract_formats_from_html(html_content, soup)
        
        # Also extract formats from filenames
        if result['file_name']:
            for filename in result['file_name'].split(', '):
                ext_match = re.search(r'\.([a-z0-9]+)$', filename, re.I)
                if ext_match:
                    formats.add(ext_match.group(1).upper())
        
        if formats:
            result['file_formats'] = ', '.join(sorted(formats)[:MAX_FORMATS_TO_DISPLAY])
            result['fair_score'] += 1
        
        # 5. Extract license
        license_info = extract_license(page_text)
        if license_info:
            result['license'] = license_info
            result['fair_score'] += 1
        
        # 6. Set accessibility
        result['accessibility'] = 'Accessible'
        
    except requests.exceptions.Timeout:
        result['accessibility'] = 'Error: Request timeout'
        logger.warning(f"Timeout accessing {url}")
    except requests.exceptions.RequestException as e:
        result['accessibility'] = f'Error: {str(e)[:100]}'
        logger.warning(f"Request error for {url}: {e}")
    except Exception as e:
        result['accessibility'] = f'Parse Error: {str(e)[:100]}'
        logger.error(f"Unexpected error for {url}: {e}")
    
    return result


# ---------- Main processing function ----------
def process_data_links(input_csv: str) -> None:
    """Process data links from the input CSV and assess FAIRness.
    
    Args:
        input_csv: Path to CSV file containing paper metadata with data_links column
    """
    input_path = Path(input_csv)
    if not input_path.exists():
        logger.error(f"Input CSV not found: {input_csv}")
        sys.exit(1)
    
    # Create output directory
    output_dir = Path("analysis/data_fair_assessment")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Processing input file: {input_csv}")
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_basename = input_path.stem
    output_csv = output_dir / f"{timestamp}_{input_basename}_fair_assessment.csv"
    
    # Read input CSV
    papers = []
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('data_links'):
                    papers.append(row)
    except Exception as e:
        logger.error(f"Error reading input CSV: {e}")
        sys.exit(1)
    
    logger.info(f"Found {len(papers)} papers with data links")
    
    # Process each paper's data links
    results = []
    for i, paper in enumerate(papers, 1):
        paper_doi = paper.get('doi', '')
        title = paper.get('title', 'Unknown')
        data_links = paper.get('data_links', '')
        
        logger.info(f"[{i}/{len(papers)}] Processing: {title[:60]}...")
        
        # Split multiple links (separated by semicolon)
        links = [link.strip() for link in data_links.split(';') if link.strip()]
        
        for link in links:
            logger.info(f"  Assessing: {link}")
            assessment = assess_dataset_url(link, paper_doi)
            
            results.append({
                'paper_doi': paper_doi,
                'paper_title': title,
                'data_link': link,
                'dataset_doi': assessment['dataset_doi'],
                'file_name': assessment['file_name'],
                'repository': assessment['repository'],
                'file_formats': assessment['file_formats'],
                'license': assessment['license'],
                'accessibility': assessment['accessibility'],
                'fair_score': assessment['fair_score']
            })
            
            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)
    
    # Write results to CSV
    fieldnames = [
        'paper_doi', 'paper_title', 'data_link', 'dataset_doi', 'file_name',
        'repository', 'file_formats', 'license', 'accessibility', 'fair_score'
    ]
    
    try:
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Results saved to: {output_csv}")
    except Exception as e:
        logger.error(f"Error writing output CSV: {e}")
        sys.exit(1)
    
    # Summary statistics
    if results:
        avg_score = sum(r['fair_score'] for r in results) / len(results)
        max_score_entry = max(results, key=lambda x: x['fair_score'])
        
        logger.info("\n" + "="*50)
        logger.info("FAIR Assessment Summary")
        logger.info("="*50)
        logger.info(f"Total datasets assessed: {len(results)}")
        logger.info(f"Average FAIR score: {avg_score:.2f}/4")
        logger.info(f"Highest scoring dataset ({max_score_entry['fair_score']}/4):")
        logger.info(f"  Repository: {max_score_entry['repository']}")
        logger.info(f"  File formats: {max_score_entry['file_formats']}")
        logger.info(f"  License: {max_score_entry['license']}")
        logger.info(f"  Accessibility: {max_score_entry['accessibility']}")
        logger.info("="*50)
    else:
        logger.warning("No datasets were assessed")


# ---------- CLI ----------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python code/data_fair_assessment.py <input_csv>")
        print("Example: python code/data_fair_assessment.py analysis/next_latest/2025_11/test-1_data.csv")
        sys.exit(1)
    
    process_data_links(sys.argv[1])
