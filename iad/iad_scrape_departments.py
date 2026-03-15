import json
import base64
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException

LOCATIONS_FILE = "locations.txt"
FINAL_FILE     = "IAD_final.json"
FAILED_FILE    = "iad_dept_failed.json"

SECTOR_URL     = "https://www.iadfrance.fr/api/agents/sector/{}?page={}&locale=fr"
AGENT_URL      = "https://www.iadfrance.fr/api/agents/{}?locale=fr"
PROXY          = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

WORKERS        = 15
RETRIES        = 3
SAVE_EVERY     = 50

headers = {
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.iadfrance.fr/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

lock          = threading.Lock()
new_agents    = []
failed_agents = []
save_counter  = 0


# ── helpers ──────────────────────────────────────────────────────────────────

def decode_phone(hashed):
    try:
        return base64.b64decode(hashed).decode()
    except Exception:
        return None


def make_session():
    return requests.Session(impersonate="chrome", proxy=PROXY)


def fetch_with_retry(session, url, retries=RETRIES):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
        except RequestException as e:
            print(f"  Retry {attempt+1}/{retries}: {e}")
        time.sleep(2 ** attempt)
    return None


# ── sector fetching (sequential — only ~100 depts) ───────────────────────────

def fetch_sector_agents(slug):
    session  = make_session()
    all_items = []
    page      = 1

    while True:
        url = SECTOR_URL.format(slug, page)
        data = None
        for attempt in range(RETRIES):
            try:
                r = session.get(url, headers=headers, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    break
            except RequestException as e:
                print(f"  Sector retry {attempt+1}/{RETRIES} [{slug} p{page}]: {e}")
            time.sleep(2)

        if not data:
            print(f"  FAILED sector: {slug} page {page}")
            break

        items = data.get("items", [])
        all_items.extend(items)

        total_items    = data.get("totalItems", 0)
        items_per_page = data.get("itemsPerPage", len(items)) or len(items)
        total_pages    = -(-total_items // items_per_page)  # ceiling division

        print(f"    page {page}/{total_pages} — {len(items)} items ({len(all_items)}/{total_items})")

        if page >= total_pages or not items:
            break
        page += 1
        time.sleep(0.3)

    return all_items


# ── per-agent enrichment + transform ─────────────────────────────────────────

def enrich_and_transform(raw_agent):
    """Enrich a raw sector-API agent and return IAD_final.json row."""
    session  = make_session()
    username = raw_agent.get("userName", "")
    data     = fetch_with_retry(session, AGENT_URL.format(username))

    # -- city / postal_code / rsac from agent-info API
    city        = []
    postal_code = ""
    rsac_number = ""

    if data:
        location = data.get("location") or {}
        legal    = data.get("legal") or {}
        rsac     = legal.get("rsac") or {}

        loc_place  = location.get("place")
        rsac_place = rsac.get("place")

        if loc_place and rsac_place and loc_place != rsac_place:
            city = [loc_place, rsac_place]
        elif loc_place:
            city = [loc_place]

        postal_code = location.get("postcode") or ""
        rsac_number = rsac.get("number") or ""

    # -- base fields from sector API
    full_name = raw_agent.get("fullName", "")
    parts     = full_name.split(" ", 1)
    first     = parts[0] if parts else ""
    last      = parts[1] if len(parts) > 1 else ""

    hashed      = (raw_agent.get("directContact") or {}).get("hashedPhone")
    phone       = decode_phone(hashed) if hashed else ""
    reviews     = raw_agent.get("reviewsCount")
    rating      = raw_agent.get("reviewsRatingAverage")
    listings    = raw_agent.get("propertyCount")

    return {
        "first_name":        first,
        "last_name":         last,
        "network":           "IAD",
        "phone_number":      phone or "",
        "city":              city,
        "postal_code":       postal_code,
        "profile_url":       f"https://www.iadfrance.fr/conseiller-immobilier/{username}",
        "number_of_listings": listings if listings is not None else "",
        "number_of_reviews":  reviews  if reviews  is not None else "",
        "average_rating":     rating   if rating   is not None else "",
        "rsac_number":       rsac_number,
        "email_address":     "",
        "source_url":        raw_agent.get("_source_url", ""),
    }


def process_agent(raw_agent):
    global save_counter
    try:
        row = enrich_and_transform(raw_agent)
        with lock:
            new_agents.append(row)
            save_counter += 1
            username = raw_agent.get("userName", "?")
            print(f"  + {username} | city={row['city']} rsac={row['rsac_number']}")
            if save_counter % SAVE_EVERY == 0:
                save_final()
    except Exception as e:
        with lock:
            failed_agents.append(raw_agent.get("userName", "?"))
            print(f"  ERROR {raw_agent.get('userName')}: {e}")


# ── save ─────────────────────────────────────────────────────────────────────

def save_final():
    with open(FINAL_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_rows + new_agents, f, ensure_ascii=False, indent=4)
    print(f"  -> Saved ({len(existing_rows) + len(new_agents)} total)")


# ── main ─────────────────────────────────────────────────────────────────────

# Load existing final file
with open(FINAL_FILE, "r", encoding="utf-8") as f:
    existing_rows = json.load(f)

existing_ids = set()
for row in existing_rows:
    url = row.get("profile_url", "")
    if url:
        existing_ids.add(url.rstrip("/").split("/")[-1])  # userName

print(f"Existing agents in {FINAL_FILE}: {len(existing_rows)}")

# Parse slugs from locations.txt
slugs = []
with open(LOCATIONS_FILE, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line.startswith("http") and "/trouver-un-conseiller/" in line:
            slug = line.rstrip("/").split("/")[-1]
            slugs.append(slug)

print(f"Departments to process: {len(slugs)}")

# Collect all new agents across departments
# Also track source_url for every username seen (existing + new)
pending = []
seen_in_run = set()
username_to_source = {}  # username -> source_url (for backfilling existing rows)

for i, slug in enumerate(slugs, 1):
    source_url = f"https://www.iadfrance.fr/trouver-un-conseiller/{slug}"
    print(f"[{i}/{len(slugs)}] Fetching sector: {slug}")
    agents = fetch_sector_agents(slug)
    dept_new = 0
    for agent in agents:
        username = agent.get("userName", "")
        if not username:
            continue
        # record source for backfill (first-seen slug wins)
        username_to_source.setdefault(username, source_url)
        if username in existing_ids or username in seen_in_run:
            continue
        seen_in_run.add(username)
        agent["_source_url"] = source_url
        pending.append(agent)
        dept_new += 1
    print(f"  -> {len(agents)} agents, {dept_new} new")
    time.sleep(0.3)

# Backfill source_url into existing rows that are missing it
backfilled = 0
for row in existing_rows:
    if row.get("source_url"):
        continue
    url = row.get("profile_url", "")
    username = url.rstrip("/").split("/")[-1] if url else ""
    src = username_to_source.get(username, "")
    if src:
        row["source_url"] = src
        backfilled += 1
print(f"Backfilled source_url for {backfilled} existing agents")

print(f"\nTotal new agents to enrich: {len(pending)}")

# Enrich new agents in parallel
with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [executor.submit(process_agent, a) for a in pending]
    for future in as_completed(futures):
        future.result()

# Final save
save_final()

with open(FAILED_FILE, "w", encoding="utf-8") as f:
    json.dump(failed_agents, f, ensure_ascii=False, indent=2)

print(f"\nDone. New agents added: {len(new_agents)} | Failed: {len(failed_agents)}")
print(f"Total in {FINAL_FILE}: {len(existing_rows) + len(new_agents)}")
