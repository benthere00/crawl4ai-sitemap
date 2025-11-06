#!/usr/bin/env python3
import os
import json
import time
import shutil
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# -----------------------------
# LOAD CONFIG
# -----------------------------
def load_config(path="config.json"):
    if not os.path.exists(path):
        print("⚠️ config.json not found — using default settings. Crawl limited to local defaults.")
        return {
            "sitemap_urls": [],
            "clean_data": True,
            "max_urls": 20,
            "crawl_delay": 0.8,
            "css_selector": "#primary,.entry-content,main",
            "trigger": False,
            "include_links_header": False,
            "save_links_file": True,
        }

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

DATA_DIR = "data"
SITEMAP_URLS = (
    [url.strip() for url in config.get("sitemap_urls", []) if url.strip()]
    if isinstance(config.get("sitemap_urls"), list)
    else [u.strip() for u in config.get("sitemap_urls", "").split(",") if u.strip()]
)
CLEAN_DATA = bool(config.get("clean_data", True))
MAX_URLS = int(config.get("max_urls", 20))
CRAWL_DELAY = float(config.get("crawl_delay", 0.8))
CSS_SELECTOR = config.get("css_selector", "#primary,.entry-content,main")
USER_AGENT = config.get("user_agent", "Crawl4AI-GitHubAction/1.0 (+https://github.com/yourname/Crawl4AI)")
INCLUDE_LINKS_HEADER = bool(config.get("include_links_header", False))
SAVE_LINKS_FILE = bool(config.get("save_links_file", True))

HEADERS = {"User-Agent": USER_AGENT}

SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".ico", ".pdf", ".zip", ".rar", ".7z", ".mp3", ".mp4",
    ".avi", ".mov", ".wmv", ".ogg", ".webm", ".json", ".xml",
    ".txt", ".csv", ".js", ".css", ".woff", ".woff2", ".ttf"
)

LINKS_FILE = "links.txt"

# -----------------------------
# UTILS
# -----------------------------
def url_to_filename(url):
    parsed = urlparse(url)
    path = parsed.path.strip("/") or "index"
    filename = (
        path.replace("/", "_")
        .replace("?", "_")
        .replace("&", "_")
        .replace("=", "_")
        .replace("%", "_")
    )
    return f"{filename}.md"

def clean_domain_folder(domain):
    folder = os.path.join(DATA_DIR, "v1", domain)
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)
    return folder

def expand_sitemap_url(sitemap_url, depth=0, max_depth=3):
    """Recursively parse sitemap and nested indexes"""
    urls = []
    indent = "  " * depth
    try:
        print(f"{indent}🔍 Fetching sitemap: {sitemap_url}")
        resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"{indent}⚠️  Sitemap fetch failed ({resp.status_code})")
            return []

        soup = BeautifulSoup(resp.text, "xml")

        # Handle nested sitemap indexes
        if soup.find("sitemapindex"):
            sitemap_locs = [loc.text.strip() for loc in soup.find_all("loc")]
            print(f"{indent}🗂 Found {len(sitemap_locs)} nested sitemaps")
            for sm in sitemap_locs:
                urls.extend(expand_sitemap_url(sm, depth + 1, max_depth))

        # Handle URL sets (ignores <image:image> entries)
        elif soup.find("urlset"):
            url_locs = []
            for url_tag in soup.find_all("url"):
                loc_tag = url_tag.find("loc", recursive=False)
                if loc_tag and loc_tag.text:
                    loc = loc_tag.text.strip()
                    if not any(loc.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
                        url_locs.append(loc)
            print(f"{indent}🌐 Found {len(url_locs)} valid HTML URLs in sitemap")
            urls.extend(url_locs)

    except Exception as e:
        print(f"{indent}⚠️  Failed to expand sitemap {sitemap_url}: {e}")

    return urls

def get_urls_from_config():
    urls = []
    for sitemap in SITEMAP_URLS:
        urls += expand_sitemap_url(sitemap)
    return urls

def is_html_url(url):
    path = urlparse(url).path.lower()
    return not path.endswith(SKIP_EXTENSIONS)

def scrape_content(html):
    soup = BeautifulSoup(html, "html.parser")
    selectors = [s.strip() for s in CSS_SELECTOR.split(",") if s.strip()]
    for sel in selectors:
        selected = soup.select(sel)
        if selected:
            return "\n\n".join([el.get_text(separator=" ", strip=True) for el in selected])
    return soup.get_text(separator=" ", strip=True)

def save_markdown(url, content):
    domain = urlparse(url).netloc.replace("www.", "")
    save_dir = os.path.join(DATA_DIR, "v1", domain)
    os.makedirs(save_dir, exist_ok=True)
    filename = url_to_filename(url)
    path = os.path.join(save_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        if INCLUDE_LINKS_HEADER:
            f.write(f"# {url}\n\n")
        f.write(content)
    print(f"✅ Saved: {path}")

def write_links_file(urls):
    """Overwrite links.txt with all crawled URLs"""
    if not SAVE_LINKS_FILE:
        return
    if os.path.exists(LINKS_FILE):
        os.remove(LINKS_FILE)
    with open(LINKS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(urls))
    print(f"📄 links.txt updated with {len(urls)} URLs")

# -----------------------------
# MAIN CRAWLER
# -----------------------------
def crawl():
    urls = get_urls_from_config()
    if not urls:
        print("⚠️  No URLs found to crawl.")
        return

    first_domain = urlparse(urls[0]).netloc.replace("www.", "")
    if CLEAN_DATA:
        clean_domain_folder(first_domain)

    # Always refresh links.txt
    write_links_file(urls)

    print(f"🌐 Starting crawl for {len(urls)} URLs (limit {MAX_URLS})")
    for i, url in enumerate(urls[:MAX_URLS], start=1):
        if not is_html_url(url):
            print(f"[{i}/{len(urls)}] Skipped non-HTML: {url}")
            continue

        print(f"[{i}/{len(urls)}] Crawling: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if "text/html" not in resp.headers.get("Content-Type", ""):
                print(f"⚠️  Non-HTML content skipped: {url}")
                continue

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
