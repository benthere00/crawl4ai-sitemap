#!/usr/bin/env python3
import os
import time
import glob
import shutil
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# -----------------------------
# CONFIGURABLE SETTINGS
# -----------------------------
DATA_DIR = "data"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", 500))
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", 0.5))
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI-GitHubAction/1.0")
CSS_SELECTOR = os.getenv("CSS_SELECTOR", "#primary")  # e.g. "#primary,.entry-content,main"

HEADERS = {"User-Agent": USER_AGENT}

# -----------------------------
# UTILS
# -----------------------------
def url_to_filename(url):
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        path = "index"
    filename = (
        path.replace("/", "_")
            .replace("?", "_")
            .replace("&", "_")
            .replace("=", "_")
            .replace("%", "_")
    )
    return f"{filename}.md"

def clean_domain_folder(domain):
    """Clean domain folder under data/v1/<domain>/"""
    folder = os.path.join(DATA_DIR, "v1", domain)
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)
    return folder

def expand_sitemap_url(sitemap_url):
    """Return list of URLs found inside sitemap.xml"""
    try:
        resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text.strip() for loc in soup.find_all("loc")]
        return locs
    except Exception as e:
        print(f"⚠️  Failed to expand sitemap {sitemap_url}: {e}")
        return []

def get_urls_from_file():
    urls = []
    if not os.path.exists(SITEMAP_FILE):
        print(f"❌ {SITEMAP_FILE} not found")
        return urls

    with open(SITEMAP_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.endswith(".xml"):
                urls += expand_sitemap_url(line)
            else:
                urls.append(line)
    return urls

def scrape_content(html):
    soup = BeautifulSoup(html, "html.parser")

    if CSS_SELECTOR:
        selectors = [s.strip() for s in CSS_SELECTOR.split(",") if s.strip()]
        for sel in selectors:
            selected = soup.select(sel)
            if selected:
                return "\n\n".join([el.get_text(separator=" ", strip=True) for el in selected])

    # fallback if no selector found
    return soup.get_text(separator=" ", strip=True)

def save_markdown(url, content):
    domain = urlparse(url).netloc.replace("www.", "")
    save_dir = os.path.join(DATA_DIR, "v1", domain)
    os.makedirs(save_dir, exist_ok=True)

    filename = url_to_filename(url)
    path = os.path.join(save_dir, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {url}\n\n{content}")
    print(f"✅ Saved: {path}")

# -----------------------------
# MAIN CRAWLER
# -----------------------------
def crawl():
    urls = get_urls_from_file()
    if not urls:
        print("⚠️  No URLs found to crawl.")
        return

    first_domain = urlparse(urls[0]).netloc.replace("www.", "")
    if CLEAN_DATA:
        clean_domain_folder(first_domain)

    print(f"🌐 Starting crawl for {len(urls)} URLs (limit {MAX_URLS})")
    for i, url in enumerate(urls[:MAX_URLS], start=1):
        print(f"[{i}/{len(urls)}] Crawling: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"⚠️  Skipped ({resp.status_code}): {url}")
                continue

            content = scrape_content(resp.text)
            save_markdown(url, content)
            time.sleep(CRAWL_DELAY)
        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")
            continue

    print("✅ Crawl complete.")

# -----------------------------
# ENTRYPOINT
# -----------------------------
if __name__ == "__main__":
    crawl()
