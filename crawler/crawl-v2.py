#!/usr/bin/env python3
import os
import time
import asyncio
import aiofiles
import urllib.parse
import xml.etree.ElementTree as ET
import requests
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import shutil

# === CONFIGURATION ===
DATA_DIR = "data"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI-GitHubAction/1.0 (+https://github.com/)")
CSS_SELECTOR = os.getenv("CSS_SELECTOR", "#primary")  # e.g. "#primary,.entry-content,main"

SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".ico", ".pdf", ".zip", ".rar", ".7z", ".mp3", ".mp4",
    ".avi", ".mov", ".wmv", ".ogg", ".webm", ".json", ".xml",
    ".txt", ".csv", ".js", ".css", ".woff", ".woff2", ".ttf"
)

# === HELPER FUNCTIONS ===
def domain_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.replace("www.", "")

def safe_filename(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    if not path:
        path = "index"
    filename = (
        path.replace("/", "_")
        .replace("?", "_")
        .replace("&", "_")
        .replace("=", "_")
        .replace("%", "_")
    )
    return filename + ".md"

def parse_sitemap(url: str) -> list:
    """Parse a sitemap.xml and return a list of URLs."""
    print(f"🧭 Expanding sitemap: {url}")
    try:
        headers = {"User-Agent": USER_AGENT}
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        urls = []
        root = ET.fromstring(res.content)
        for loc in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            if loc.text:
                urls.append(loc.text.strip())
        print(f"   Found {len(urls)} URLs in sitemap.")
        return urls
    except Exception as e:
        print(f"⚠️ Failed to parse sitemap {url}: {e}")
        return []

def is_html_url(url: str) -> bool:
    """Skip obvious non-HTML URLs."""
    lower_path = urllib.parse.urlparse(url).path.lower()
    return not lower_path.endswith(SKIP_EXTENSIONS)

def extract_content(html: str) -> str:
    """Extract content based on CSS selectors, fallback to full text."""
    soup = BeautifulSoup(html, "html.parser")

    if CSS_SELECTOR:
        selectors = [s.strip() for s in CSS_SELECTOR.split(",") if s.strip()]
        for sel in selectors:
            selected = soup.select(sel)
            if selected:
                return "\n\n".join([el.get_text(separator=" ", strip=True) for el in selected])

    # fallback — grab everything
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    return soup.get_text(separator=" ", strip=True)

async def crawl_url(crawler, url, save_dir):
    """Crawl a single URL and save its markdown output."""
    try:
        result = await crawler.arun(
            url=url,
            headers={"User-Agent": USER_AGENT},
        )
        html_content = result.html or ""
        text_content = extract_content(html_content)
        filename = safe_filename(url)
        filepath = os.path.join(save_dir, filename)

        async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
            await f.write(f"# {url}\n\n{text_content}")

        print(f"✅ Saved {filepath}")
    except Exception as e:
        print(f"❌ Error crawling {url}: {e}")

async def main():
    if not os.path.exists(SITEMAP_FILE):
        print(f"❌ Missing {SITEMAP_FILE}")
        return

    # Collect all URLs from sitemaps.txt
    with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
        sitemap_urls = [line.strip() for line in f if line.strip()]

    all_urls = []
    for sitemap in sitemap_urls:
        if sitemap.endswith(".xml"):
            all_urls.extend(parse_sitemap(sitemap))
        else:
            all_urls.append(sitemap)

    if not all_urls:
        print("⚠️ No URLs found to crawl.")
        return

    # Remove duplicates and limit
    all_urls = list(dict.fromkeys(all_urls))[:MAX_URLS]

    first_domain = domain_from_url(all_urls[0])
    domain_dir = os.path.join(DATA_DIR, "v2", first_domain)

    # Clean only that domain folder
    if CLEAN_DATA and os.path.exists(domain_dir):
        print(f"🧹 Cleaning old data in {domain_dir} ...")
        shutil.rmtree(domain_dir)
    os.makedirs(domain_dir, exist_ok=True)

    print(f"🚀 Starting crawl for {len(all_urls)} URLs under {first_domain} ...")
    print(f"   Using User-Agent: {USER_AGENT}")
    print(f"   Crawl delay: {CRAWL_DELAY}s")
    print(f"   CSS Selectors: {CSS_SELECTOR or '(none, full-page fallback)'}")

    async with AsyncWebCrawler() as crawler:
        for i, url in enumerate(all_urls, start=1):
            if not is_html_url(url):
                print(f"[{i}/{len(all_urls)}] Skipped non-HTML: {url}")
                continue

            print(f"\n[{i}/{len(all_urls)}] Crawling: {url}")
            await crawl_url(crawler, url, domain_dir)
            if i < len(all_urls):
                await asyncio.sleep(CRAWL_DELAY)

    print(f"\n✅ Crawl completed. Saved files under {domain_dir}")

if __name__ == "__main__":
    asyncio.run(main())
