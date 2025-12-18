# fair_champion package config
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path.home() / ".config" / "api_keys.env")

ELSEVIER_KEY = os.getenv("ELSEVIER_API_KEY")
SPRINGER_META_KEY = os.getenv("SPRINGER_META_API_KEY")  # Used for /meta/v2/ endpoints
SPRINGER_OA_KEY = os.getenv("SPRINGER_OA_API_KEY")  # Used for /openaccess/ endpoints
WILEY_KEY = os.getenv("WILEY_API_KEY")

PROXY_DOI_TEMPLATE = os.getenv("PROXY_DOI_TEMPLATE")  # must include %s for URL
EZPROXY_PREFIX = os.getenv("EZPROXY_PREFIX")

OUTPUT_BASE = Path("analysis") / "fair_champion"
OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
