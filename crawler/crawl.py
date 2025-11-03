#!/usr/bin/env python3
import os
import requests
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# === Config ===
DATA_DIR = "data/v1"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI/1.0 (+https://github.com/yourname/Crawl4AI)")
DELAY = float(os.getenv("CRAWL_DELAY", 0.5))
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CSS_SELECTORS = [
    s.strip()
    for s in os.getenv("CSS_SELECTOR", "").split(",")
    if s.strip()
]


def clean_data_folder(domain_dir):
    """Clean only the current domain’s folder."""
    if os.path.exists(domain_dir) and CLEAN_DATA:
        import shutil
        print(f"🧹 Cleaning existing {domain_dir}/ directory...")
        shutil.rmtree(domain_dir, ignore_errors=True)
    os.makedirs(domain_dir, exist_ok=True)


def load_sitemaps():
    if not os.path.exists(SITEMAP_FILE):
        raise FileNotFoundError(f"Missing sitemap list: {SITEMAP_FILE}")
    with open(SITEMAP_FILE) as f:
        sitemaps = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    print(f"🗺️ Loaded {len(sitemaps)} sitemap(s)")
    return sitemaps


def parse_sitemap(url):
    print(f"🧭 Fetching sitemap: {url}")
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
        print(f"✅ Found {len(urls)} URLs in {url}")
        return urls
    except Exception as e:
        print(f"⚠️ Error parsing sitemap {url}: {e}")
        return []


def fetch_page(url):
    """Download and extract text using CSS selectors or fallback."""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove irrelevant tags
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()

        title = soup.title.string.strip() if soup.title and soup.title.string else url

        text_content = ""
        used_selector = None

        # Try each selector if specified
        if CSS_SELECTORS:
            for selector in CSS_SELECTORS:
                selected = soup.select(selector)
                if selected:
                    text_content = " ".join([el.get_text(strip=True) for el in selected])
                    used_selector = selector
                    break

        # Fallback if nothing matched or no selector provided
        if not text_content:
            text_content = " ".join(soup.get_text().split())

        return title, text_content, used_selector

    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None, None, None


def url_to_filename(url):
    parsed = urlparse(url)
    slug = parsed.netloc + parsed.path
    if slug.endswith("/"):
        slug = slug[:-1]
    slug = slug.replace("/", "_")
    if not slug:
        slug = "index"
    return f"{slug}.md"


def save_markdown(url, title, text, save_dir, selector_used=None):
    filename = url_to_filename(url)
    path = os.path.join(save_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"**URL:** {url}\n\n")
        if selector_used:
            f.write(f"**Selector Used:** `{selector_used}`\n\n")
        f.write(text)
    print(f"✅ Saved: {path}")


def crawl():
    start = time.time()
    sitemaps = load_sitemaps()

    all_urls = []
    for sm in sitemaps:
        all_urls.extend(parse_sitemap(sm))

    if not all_urls:
        print("⚠️ No URLs found. Exiting.")
        return

    all_urls = all_urls[:MAX_URLS]
    print(f"🌐 Total URLs to crawl: {len(all_urls)}")

    first_domain = urlparse(all_urls[0]).netloc.replace("www.", "")
    domain_dir = os.path.join(DATA_DIR, first_domain)
    os.makedirs(domain_dir, exist_ok=True)
    clean_data_folder(domain_dir)

    for i, url in enumerate(all_urls, start=1):
        if url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".mp4")):
            continue
        print(f"\n[{i}/{len(all_urls)}] Crawling: {url}")
        title, text, selector_used = fetch_page(url)
        if title and text:
            save_markdown(url, title, text, domain_dir, selector_used)
        time.sleep(DELAY)

    duration = time.time() - start
    print(f"\n✅ Crawl finished in {duration:.2f}s, results saved under '{domain_dir}/'.")


if __name__ == "__main__":
    crawl()
