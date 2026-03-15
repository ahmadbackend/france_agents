"""
Efficity department scraper
- Reads every department URL from routes.txt
- Paginates each listing page to collect all agent profile links
- Scrapes each agent profile (JSON-LD) in parallel threads
- Skips agents already in efficity_final.json
- Backfills source_url into pre-existing agents
- Records failed links to efficity_failed.txt
- Checkpoints every SAVE_EVERY agents
"""

import json
import re
import time
import threading
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

# ── config ────────────────────────────────────────────────────────────────────

ROUTES_FILE = "routes.txt"
FINAL_FILE  = "efficity_final.json"
FAILED_FILE = "efficity_failed.txt"

PROXY      = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
WORKERS    = 15
RETRIES    = 3
SAVE_EVERY = 50

BASE = "https://www.efficity.com"

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

# ── shared state ──────────────────────────────────────────────────────────────

lock         = threading.Lock()
new_agents   = []
save_counter = 0

# ── helpers ───────────────────────────────────────────────────────────────────

def make_session():
    return requests.Session(impersonate="chrome", proxy=PROXY)


def get_html(session, url, retries=RETRIES):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.text
            print(f"  [{r.status_code}] {url}")
        except Exception as e:
            print(f"  [ERR attempt {attempt+1}] {url}: {e}")
        time.sleep(2 ** attempt)
    return None


# ── step 1 — collect agent links from one department page (with pagination) ───

def collect_dept_links(dept_url):
    """Return all agent profile URLs listed under a department page."""
    session = make_session()
    links   = []
    page    = 1

    while True:
        # efficity pagination: ?p=N (first page has no param)
        url = dept_url if page == 1 else f"{dept_url}?p={page}"
        html = get_html(session, url)
        if not html:
            print(f"  FAILED dept page: {url}")
            break

        soup = BeautifulSoup(html, "html.parser")

        # agent cards
        page_links = []
        for a in soup.select("div.index-list-item a[href]"):
            href = a["href"]
            full = urljoin(BASE, href)
            if full not in links:          # dedup within dept
                page_links.append(full)

        links.extend(page_links)
        print(f"    {dept_url} p{page} → {len(page_links)} links (total {len(links)})")

        if not page_links:
            break

        # check for a "next page" link
        next_tag = (
            soup.find("a", rel="next") or
            soup.find("a", string=lambda t: t and ("suivant" in t.lower() or "next" in t.lower()))
        )
        if not next_tag:
            break

        page += 1
        time.sleep(0.3)

    return links


# ── step 2 — scrape one agent profile ────────────────────────────────────────

def extract_listings(soup):
    """
    Extracts listing stats from div.card-text blocks:
      "22 Properties sold", "4 Properties for Sale", "0 Properties under contract"
    Returns a list of dicts: [{"count": 22, "label": "Properties sold"}, ...]
    """
    results = []
    for card in soup.select("div.card-text"):
        p = card.find("p")
        if not p:
            continue
        text = p.get_text(" ", strip=True)
        m = re.match(r"(\d+)\s+(.+)", text)
        if m:
            results.append({"count": int(m.group(1)), "label": m.group(2).strip()})
    return results


def scrape_agent_page(session, profile_url):
    html = get_html(session, profile_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    name = phone = email = city = rating = reviews = None

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            objects = data if isinstance(data, list) else [data]
            for obj in objects:
                if obj.get("@type") == "RealEstateAgent":
                    name  = obj.get("name")
                    phone = obj.get("telephone")
                    email = obj.get("email")
                    addr  = obj.get("address") or {}
                    locality = addr.get("addressLocality")
                    region   = addr.get("addressRegion")
                    if locality and region:
                        city = f"{locality} ({region})"
                    else:
                        city = locality or region

                if obj.get("@type") == "Product":
                    agg = obj.get("aggregateRating") or {}
                    rating  = agg.get("ratingValue")
                    reviews = agg.get("reviewCount")
        except Exception:
            continue

    if not name:
        return None

    parts      = name.split(" ", 1)
    first_name = parts[0]
    last_name  = parts[1] if len(parts) > 1 else ""

    return {
        "first_name":         first_name,
        "last_name":          last_name,
        "network":            "efficity",
        "phone_number":       phone or "",
        "city":               city or "",
        "postal_code":        "",
        "profile_url":        profile_url,
        "number_of_listings": extract_listings(soup),
        "number_of_reviews":  reviews if reviews is not None else "",
        "average_rating":     rating  if rating  is not None else "",
        "rsac_number":        "",
        "email_address":      email or "",
        "source_url":         "",   # filled in by caller
    }


# ── step 3 — worker ───────────────────────────────────────────────────────────

def process_agent(profile_url, source_url):
    global save_counter
    session = make_session()
    row     = scrape_agent_page(session, profile_url)

    with lock:
        if row:
            row["source_url"] = source_url
            new_agents.append(row)
            save_counter += 1
            print(f"  + [{save_counter}] {row['first_name']} {row['last_name']} | {row['city']}")
            if save_counter % SAVE_EVERY == 0:
                _save()
        else:
            with open(FAILED_FILE, "a", encoding="utf-8") as fh:
                fh.write(profile_url + "\n")
            print(f"  FAIL {profile_url}")


def _save():
    with open(FINAL_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_rows + new_agents, f, ensure_ascii=False, indent=4)
    print(f"  -> checkpoint saved ({len(existing_rows) + len(new_agents)} total)")


# ── main ──────────────────────────────────────────────────────────────────────

# load existing
with open(FINAL_FILE, encoding="utf-8") as f:
    existing_rows = json.load(f)

existing_urls = {r["profile_url"].rstrip("/") for r in existing_rows}
print(f"Existing agents: {len(existing_rows)}")

# parse routes
dept_urls = []
with open(ROUTES_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line.startswith("http"):
            dept_urls.append(line)

print(f"Departments to process: {len(dept_urls)}")

# collect all agent links across departments, tracking source
pending            = []          # list of (profile_url, source_url)
seen_in_run        = set()
url_to_source      = {}          # for backfilling existing rows

for i, dept_url in enumerate(dept_urls, 1):
    print(f"[{i}/{len(dept_urls)}] {dept_url}")
    links = collect_dept_links(dept_url)
    dept_new = 0
    for link in links:
        norm = link.rstrip("/")
        url_to_source.setdefault(norm, dept_url)
        if norm in existing_urls or norm in seen_in_run:
            continue
        seen_in_run.add(norm)
        pending.append((link, dept_url))
        dept_new += 1
    print(f"  -> {len(links)} links, {dept_new} new")
    time.sleep(0.3)

# backfill source_url into existing rows
backfilled = 0
for row in existing_rows:
    if row.get("source_url"):
        continue
    norm = row.get("profile_url", "").rstrip("/")
    src  = url_to_source.get(norm, "")
    if src:
        row["source_url"] = src
        backfilled += 1
print(f"Backfilled source_url for {backfilled} existing agents")

print(f"\nNew agent profiles to scrape: {len(pending)}")

# scrape new agents in parallel
with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(process_agent, url, src): url for url, src in pending}
    for fut in as_completed(futures):
        fut.result()

# final save
_save()

print(f"\nDone. New: {len(new_agents)} | Total: {len(existing_rows) + len(new_agents)}")
print(f"Failed URLs logged to {FAILED_FILE}")
