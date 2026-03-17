"""
Scrape each agent's profile page and update number_of_listings
with the count from the section-title "X properties for sale by ..."

Logs:
  - efficity_listings_changes.log    → agents whose count changed (previous → current)
  - efficity_listings_no_section.log → agents where the selector returned nothing (kept unchanged)
  - efficity_update_listings_failed.txt → agents whose page failed to load
"""

import json
import re
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

# ── config ────────────────────────────────────────────────────────────────────

FINAL_FILE      = "efficity_final.json"
FAILED_FILE     = "efficity_update_listings_failed.txt"
CHANGES_LOG     = "efficity_listings_changes.log"
NO_SECTION_LOG  = "efficity_listings_no_section.log"

PROXY      = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
WORKERS    = 15
RETRIES    = 3
SAVE_EVERY = 100

# ── shared state ──────────────────────────────────────────────────────────────

lock             = threading.Lock()
completed        = 0
failed_count     = 0
changed_count    = 0
no_section_count = 0

changes_lines    = []
no_section_lines = []

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


def extract_listing_count(html):
    """
    Find <section id="section-products"> then <h2 class="section-title">
    which contains text like "4 properties for sale by Agent Name".
    Returns (count, found) where found=False means section was missing.
    """
    soup = BeautifulSoup(html, "html.parser")

    section = soup.find("section", id="section-products")
    if not section:
        return None, False

    h2 = section.find("h2", class_="section-title")
    if not h2:
        return None, False

    text = h2.get_text(" ", strip=True)
    m = re.match(r"(\d+)", text)
    if m:
        return int(m.group(1)), True

    return None, False


# ── worker ────────────────────────────────────────────────────────────────────

def process(idx, agent):
    global completed, failed_count, changed_count, no_section_count

    session     = make_session()
    profile_url = agent.get("profile_url", "")
    name        = f"{agent.get('first_name', '')} {agent.get('last_name', '')}"
    previous    = agent.get("number_of_listings")
    html        = get_html(session, profile_url)

    with lock:
        if html:
            count, found = extract_listing_count(html)
            completed += 1

            if found:
                agents[idx]["number_of_listings"] = count
                if count != previous:
                    changed_count += 1
                    line = f"{name} | {profile_url} | previous: {previous} → current: {count}"
                    changes_lines.append(line)
                    print(f"  CHANGED [{completed}/{total_todo}] {profile_url} | {previous} → {count}")
                else:
                    print(f"  OK [{completed}/{total_todo}] {profile_url} → {count} (unchanged)")
            else:
                # Section not found — keep current number
                no_section_count += 1
                line = f"{name} | {profile_url} | kept: {previous}"
                no_section_lines.append(line)
                print(f"  NO_SECTION [{completed}/{total_todo}] {profile_url} | kept {previous}")

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


def _write_logs():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(CHANGES_LOG, "w", encoding="utf-8") as f:
        f.write(f"# Listings changes log — {ts}\n")
        f.write(f"# Total changed: {changed_count}\n\n")
        for line in changes_lines:
            f.write(line + "\n")

    with open(NO_SECTION_LOG, "w", encoding="utf-8") as f:
        f.write(f"# Agents with no section-products found — {ts}\n")
        f.write(f"# Total: {no_section_count}\n\n")
        for line in no_section_lines:
            f.write(line + "\n")


# ── main ──────────────────────────────────────────────────────────────────────

with open(FINAL_FILE, encoding="utf-8") as f:
    agents = json.load(f)

todo = [(i, a) for i, a in enumerate(agents) if a.get("profile_url")]
total_todo = len(todo)

print(f"Total agents: {len(agents)} | To process: {total_todo}")

with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(process, i, a): i for i, a in todo}
    for fut in as_completed(futures):
        fut.result()

# final save + logs
_save()
_write_logs()

print(f"\nDone. Processed: {completed} | Changed: {changed_count} | No section: {no_section_count} | Failed: {failed_count}")
print(f"Logs: {CHANGES_LOG}, {NO_SECTION_LOG}, {FAILED_FILE}")
