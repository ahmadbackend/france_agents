import json
import string
import itertools
import time
import threading
from curl_cffi import requests
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.iadfrance.fr/api/locations"
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

headers = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0",
}

OUTPUT_FILE = "iad_locations.json"
MAX_WORKERS = 10        # concurrent threads
SAVE_EVERY = 50         # flush to disk every N new entries

# --- Shared state ---
all_locations = {}
seen_lock = threading.Lock()
file_lock = threading.Lock()
new_since_save = 0

def phone_number_decoder(phone):
    return base64.b64decode(phone).decode()

def load_existing():
    """Resume from existing file if present."""
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                all_locations[item["slug"]] = item
        print(f"Resumed: {len(all_locations)} existing entries loaded")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

def flush_to_disk():
    with file_lock:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(list(all_locations.values()), f, ensure_ascii=False, indent=2)

def get_locations(search_term):
    global new_since_save
    session = requests.Session()  # per-thread session
    offset = 0
    page_size = 10
    added = 0

    while True:
        params = {"locale": "fr", "search": search_term, "offset": offset}

        for attempt in range(3):  # retry up to 3x
            try:
                r = session.get(BASE_URL, headers=headers, params=params,
                                impersonate="chrome", proxy=PROXY, timeout=15)
                if r.status_code == 200:
                    break
                print(f"  [{search_term}] HTTP {r.status_code}, retry {attempt+1}")
                time.sleep(1.5 ** attempt)
            except Exception as e:
                print(f"  [{search_term}] Error: {e}, retry {attempt+1}")
                time.sleep(1.5 ** attempt)
        else:
            print(f"  [{search_term}] Failed after 3 attempts, skipping offset {offset}")
            break  # skip this page but keep what we got so far

        data = r.json()
        items = data.get("items", [])
        if not items:
            break

        with seen_lock:
            for item in items:
                key = item["slug"]
                if key not in all_locations:
                    all_locations[key] = item
                    added += 1
                    new_since_save += 1

            if new_since_save >= SAVE_EVERY:
                flush_to_disk()
                new_since_save = 0

        offset += page_size
        if offset >= data.get("totalItems", 0):
            break

    return search_term, added


# --- Main ---
load_existing()

combos = ["".join(c) for c in itertools.product(string.ascii_lowercase, repeat=3)]

# Skip already-covered combos (optional: track done set for true resume)
print(f"Total combos to search: {len(combos)}")

done = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(get_locations, s): s for s in combos}
    for future in as_completed(futures):
        search_term, added = future.result()
        done += 1
        if added or done % 100 == 0:
            print(f"[{done}/{len(combos)}] '{search_term}' +{added} | total: {len(all_locations)}")

# Final save
flush_to_disk()
print(f"Done. Saved {len(all_locations)} locations to {OUTPUT_FILE}")
#agent info api end point /api/agents/irene.valentin?locale=fr

#sector(location) agents endpoint /api/agents/sector/parata-20229?locale=fr