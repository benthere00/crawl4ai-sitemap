#!/usr/bin/env python3
import os
import time
import random
import requests
import xml.etree.ElementTree as ET
from crawl4ai import WebCrawler
from dotenv import load_dotenv

# === Load .env or GitHub Actions env ===
load_dotenv()

SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
OUTPUT_DIR = "data"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"

# === Prepare output folder ===
if CLEAN_DATA and os.path.exists(OUTPUT_DIR):
    print(f"🧹 Cleaning old data in {OUTPUT_DIR}/ ...")
    for root, _, files in os.walk(OUTPUT_DIR):
        for f in files:
            os.remove(os.path.join(root, f))
os.makedirs(OUTPUT_DIR, exist_ok=True)


def expand_sitemap(url):
    """Read a sitemap.xml URL and return a list of page URLs."""
    urls = []
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()

        root = ET.fromstring(res.text)
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Try <url><loc> pattern
        for loc in root.findall(".//ns:loc", namespace):
            urls.append(loc.text.strip())

        # Nested sitemap (index of sitemaps)
        if not urls and root.findall(".//ns:sitemap", namespace):
            for sm in root.findall(".//ns:sitemap/ns:loc", namespace):
                nested = expand_sitemap(sm.text.strip())
                urls.extend(nested)

    except Exception as e:
        print(f"⚠️ Failed to expand sitemap {url}: {e}")
    return urls


def load_sitemap_list():
    """Load sitemaps.txt and expand XMLs."""
    all_urls = []
    with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.endswith(".xml"):
                print(f"📜 Expanding sitemap XML: {line}")
                urls = expand_sitemap(line)
                print(f"  → Found {len(urls)} URLs")
                all_urls.extend(urls)
            else:
                all_urls.append(line)

    # Deduplicate
    unique = list(dict.fromkeys(all_urls))
    print(f"✅ Total URLs loaded (deduped): {len(unique)}")
    return unique[:MAX_URLS]


def main():
    urls = load_sitemap_list()
    if not urls:
        print("⚠️ No URLs found to crawl. Exiting.")
        return

    crawler = WebCrawler()

    for i, url in enumerate(urls, start=1):
        print(f"\n[{i}/{len(urls)}] Crawling: {url}")
        try:
            result = crawler.run(url)
            if not result or not result.markdown:
                print("⚠️ No markdown content, skipping.")
                continue

            safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")
            file_path = os.path.join(OUTPUT_DIR, f"{safe_name}.md")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(result.markdown)
            print(f"✅ Saved → {file_path}")

        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")

        time.sleep(random.uniform(0.5, 1.5))

    print("\n🎉 Crawl complete! Markdown files in /data")


if __name__ == "__main__":
    main()
