"""
render.py

Render JavaScript-heavy publisher pages using Selenium.
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def render_html(url: str, wait=5) -> str:
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=opts)
    driver.get(url)
    time.sleep(wait)
    html = driver.page_source
    driver.quit()
    return html
