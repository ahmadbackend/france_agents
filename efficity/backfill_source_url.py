"""
Backfill source_url for agents that have an empty source_url.
- Loops every department URL from routes.txt
- Paginates each dept page to collect agent profile links
- Once an agent profile_url is found, inserts the matching dept URL
- Stops completely as soon as all empty agents are matched
"""

import json
import time
from urllib.parse import urljoin
from curl_cffi import requests
from bs4 import BeautifulSoup

# ── config ────────────────────────────────────────────────────────────────────

ROUTES_FILE = "routes.txt"
FINAL_FILE  = "efficity_final.json"

PROXY   = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
RETRIES = 3
BASE    = "https://www.efficity.com"

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
    "referer": "https://www.efficity.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

# ── helpers ───────────────────────────────────────────────────────────────────

def make_session():
    return requests.Session(impersonate="chrome", proxy=PROXY)


def get_html(session, url):
    for attempt in range(RETRIES):
        try:
            r = session.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.text
            print(f"  [{r.status_code}] {url}")
        except Exception as e:
            print(f"  [ERR attempt {attempt+1}] {url}: {e}")
        time.sleep(2 ** attempt)
    return None


# ── main ──────────────────────────────────────────────────────────────────────

with open(FINAL_FILE, encoding="utf-8") as f:
    agents = json.load(f)

# build map: normalised profile_url -> agent index, for agents missing source_url
need = {
    a["profile_url"].rstrip("/"): i
    for i, a in enumerate(agents)
    if not a.get("source_url", "").strip()
}

print(f"Total agents: {len(agents)} | Empty source_url: {len(need)}")
if not need:
    print("Nothing to do.")
    exit()

for url in need:
    print(f"  {url}")
print()

# parse dept routes
dept_urls = []
with open(ROUTES_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line.startswith("http"):
            dept_urls.append(line)

print(f"Departments to scan: {len(dept_urls)}\n")

session     = make_session()
found_count = 0
done        = False

for di, dept_url in enumerate(dept_urls, 1):
    if done:
        break

    print(f"[{di}/{len(dept_urls)}] {dept_url}")
    page = 1

    while True:
        if done:
            break

        url  = dept_url if page == 1 else f"{dept_url}?p={page}"
        html = get_html(session, url)
        if not html:
            print(f"  FAILED: {url}")
            break

        soup = BeautifulSoup(html, "html.parser")
        page_links = []

        for a_tag in soup.select("div.index-list-item a[href]"):
            href = a_tag["href"]
            full = urljoin(BASE, href)
            norm = full.rstrip("/")
            page_links.append(norm)

            if norm in need:
                idx = need.pop(norm)
                agents[idx]["source_url"] = dept_url
                found_count += 1
                print(f"  OK [{found_count}] {agents[idx]['first_name']} {agents[idx]['last_name']} -> {dept_url}")

                if not need:
                    print("\nAll targets matched — stopping.")
                    done = True
                    break

        print(f"    p{page} -> {len(page_links)} links | remaining targets: {len(need)}")

        if not page_links:
            break

        next_tag = (
            soup.find("a", rel="next") or
            soup.find("a", string=lambda t: t and ("suivant" in t.lower() or "next" in t.lower()))
        )
        if not next_tag:
            break

        page += 1
        time.sleep(0.3)

    time.sleep(0.3)

# save
with open(FINAL_FILE, "w", encoding="utf-8") as f:
    json.dump(agents, f, ensure_ascii=False, indent=4)

print(f"\nDone. Updated: {found_count}")
if need:
    print(f"Still unmatched ({len(need)}):")
    for norm, idx in need.items():
        print(f"  [{idx}] {agents[idx]['first_name']} {agents[idx]['last_name']} | {norm}")
