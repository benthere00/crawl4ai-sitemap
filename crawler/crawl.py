#!/usr/bin/env python3
import os
import requests
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import shutil

# === Config ===
BASE_DATA_DIR = os.path.join("data", "v1")
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
USER_AGENT = os.getenv("USER_AGENT", "CrawlLite/2.0 (+https://github.com/<yourname>/Craw4I)")
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", 0.5))
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", "0"))  # 0 = unlimited

# === Helpers ===
def domain_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "")

def get_domain_dir(domain: str) -> str:
    """Return and ensure the data/v1/<domain> folder exists, cleaning if needed."""
    folder = os.path.join(BASE_DATA_DIR, domain)
    os.makedirs(folder, exist_ok=True)

    if CLEAN_DATA:
        # Only clean contents if the folder already exists and CLEAN_DATA=true
        print(f"🧹 Cleaning data folder: {folder}")
        for root, dirs, files in os.walk(folder):
            for f in files:
                try:
                    os.remove(os.path.join(root, f))
                except Exception:
                    pass
            for d in dirs:
                try:
                    shutil.rmtree(os.path.join(root, d))
                except Exception:
                    pass
    return folder

def load_sitemaps():
    if not os.path.exists(SITEMAP_FILE):
        raise FileNotFoundError(f"❌ Missing sitemap list: {SITEMAP_FILE}")
    with open(SITEMAP_FILE) as f:
        sitemaps = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    print(f"🗺️ Loaded {len(sitemaps)} sitemap(s)")
    return sitemaps

def parse_sitemap(url):
    print(f"🌐 Fetching sitemap: {url}")
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
    print(f"✅ Found {len(urls)} URLs in {url}")
    return urls

def fetch_page(url):
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=25)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.extract()

        title = soup.title.string.strip() if soup.title and soup.title.string else url
        text = " ".join(soup.get_text().split())
        return title, text
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None, None

def url_to_filename(url):
    parsed = urlparse(url)
    slug = parsed.path.strip("/")
    if not slug:
        slug = "index"
    slug = slug.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
    return f"{slug}.md"

def save_markdown(folder, url, title, text):
    filename = url_to_filename(url)
    path = os.path.join(folder, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"**URL:** {url}\n\n")
        f.write(text)
    print(f"💾 Saved: {path}")

# === Main crawler ===
def crawl():
    start = time.time()
    sitemaps = load_sitemaps()

    all_urls = []
    for sm in sitemaps:
        all_urls.extend(parse_sitemap(sm))

    if not all_urls:
        print("⚠️ No URLs found. Exiting.")
        return

    if MAX_URLS > 0:
        all_urls = all_urls[:MAX_URLS]

    first_domain = domain_from_url(all_urls[0])
    save_dir = get_domain_dir(first_domain)

    print(f"🚀 Starting crawl for {len(all_urls)} pages under {first_domain}")
    print(f"   User-Agent: {USER_AGENT}")
    print(f"   Crawl Delay: {CRAWL_DELAY}s\n")

    for i, url in enumerate(all_urls, start=1):
        if url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".mp4")):
            continue
        print(f"[{i}/{len(all_urls)}] Crawling: {url}")
        title, text = fetch_page(url)
        if title and text:
            save_markdown(save_dir, url, title, text)
        time.sleep(CRAWL_DELAY)

    duration = time.time() - start
    print(f"\n✅ Crawl finished in {duration:.1f}s. Markdown files saved under '{save_dir}/'.")

if __name__ == "__main__":
    crawl()
