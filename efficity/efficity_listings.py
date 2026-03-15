"""
Efficity listing enricher
- Loops over every agent in efficity_final.json
- Fetches their profile page and extracts listing stats from div.card-text blocks:
    "22 Properties sold", "4 Properties for Sale", "0 Properties under contract"
- Replaces number_of_listings with an array of dicts:
    [{"count": 22, "label": "Properties sold"}, ...]
- Skips agents that already have a list value
- Records failed profile URLs to efficity_listings_failed.txt
- Checkpoints every SAVE_EVERY completions
"""

import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

# ── config ────────────────────────────────────────────────────────────────────

FINAL_FILE  = "efficity_final.json"
FAILED_FILE = "efficity_listings_failed.txt"

PROXY      = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
WORKERS    = 15
RETRIES    = 3
SAVE_EVERY = 100

# ── shared state ──────────────────────────────────────────────────────────────

lock         = threading.Lock()
completed    = 0
failed_count = 0

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


def extract_listings(html):
    """
    Returns a list of dicts like:
      [{"count": 22, "label": "Properties sold"}, ...]
    from div.card-text > p elements.
    Returns None if nothing found.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for card in soup.select("div.card-text"):
        p = card.find("p")
        if not p:
            continue
        text = p.get_text(" ", strip=True)
        m = re.match(r"(\d+)\s+(.+)", text)
        if m:
            results.append({
                "count": int(m.group(1)),
                "label": m.group(2).strip(),
            })

    return results if results else None


# ── worker ────────────────────────────────────────────────────────────────────

def process(idx, agent):
    global completed, failed_count

    session      = make_session()
    profile_url  = agent.get("profile_url", "")
    html         = get_html(session, profile_url)

    with lock:
        if html:
            listings = extract_listings(html)
            agents[idx]["number_of_listings"] = listings if listings is not None else []
            completed += 1
            print(f"  OK [{completed}] {profile_url} → {listings}")

            if completed % SAVE_EVERY == 0:
                _save()
        else:
            failed_count += 1
            with open(FAILED_FILE, "a", encoding="utf-8") as fh:
                fh.write(profile_url + "\n")
            print(f"  FAIL [{failed_count}] {profile_url}")


def _save():
    with open(FINAL_FILE, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=4)
    print(f"  -> checkpoint saved ({len(agents)} agents)")


# ── main ──────────────────────────────────────────────────────────────────────

with open(FINAL_FILE, encoding="utf-8") as f:
    agents = json.load(f)

# only process agents whose number_of_listings is not already a list
todo = [
    (i, a) for i, a in enumerate(agents)
    if not isinstance(a.get("number_of_listings"), list)
]

print(f"Total agents: {len(agents)} | To enrich: {len(todo)}")

with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(process, i, a): i for i, a in todo}
    for fut in as_completed(futures):
        fut.result()

# final save
_save()

print(f"\nDone. Updated: {completed} | Failed: {failed_count}")
print(f"Failed URLs logged to {FAILED_FILE}")
