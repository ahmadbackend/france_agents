import json
import csv
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException

JSON_FILE = "IAD_final.json"
CSV_FILE  = "IAD_final.csv"

AGENT_URL = "https://www.iadfrance.fr/api/agents/{}?locale=fr"
PROXY     = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
WORKERS   = 10
RETRIES   = 3

headers = {
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.iadfrance.fr/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

lock = threading.Lock()
filled = 0
failed = 0


def make_session():
    return requests.Session(impersonate="chrome", proxy=PROXY)


def get_source_url(username):
    """Call agent API and extract source_url from breadcrumbs."""
    global filled, failed
    session = make_session()

    for attempt in range(RETRIES):
        try:
            r = session.get(AGENT_URL.format(username), headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                breadcrumbs = data.get("breadcrumbs", [])
                # The department breadcrumb is typically at index 2
                for bc in breadcrumbs:
                    to = bc.get("to") or {}
                    params = to.get("params") or {}
                    sector = params.get("agentSector", "")
                    name = to.get("name", "")
                    # We want the department-level breadcrumb, not the city one
                    if name == "find-real-estate-agent-agentSector" and not any(c.isdigit() for c in sector[-6:] if c != sector.split("-")[-1]):
                        # Check if it's a department slug (ends with dept number like "aisne-02")
                        pass
                # Simpler: find the breadcrumb whose agentSector matches a department pattern
                for bc in breadcrumbs:
                    to = bc.get("to") or {}
                    params = to.get("params") or {}
                    sector = params.get("agentSector", "")
                    if sector and sector != "":
                        # Take the first agentSector that looks like a department (region comes first, then dept)
                        # Region breadcrumb: "hauts-de-france", Dept: "aisne-02", City: "chauny-02300"
                        # We want the one ending with a 2-3 char dept code, not a 5-digit postal code
                        parts = sector.rsplit("-", 1)
                        if len(parts) == 2:
                            suffix = parts[1]
                            # Department codes: 01-95, 2A, 2B, 971-976
                            if len(suffix) <= 3 and (suffix.isdigit() or suffix in ("2A", "2B")):
                                url = f"https://www.iadfrance.fr/trouver-un-conseiller/{sector}"
                                with lock:
                                    filled += 1
                                    print(f"  OK {username} -> {sector} ({filled} done)")
                                return url
                with lock:
                    failed += 1
                    print(f"  NO DEPT breadcrumb for {username}")
                return None
        except RequestException as e:
            print(f"  Retry {attempt+1}/{RETRIES} for {username}: {e}")
            time.sleep(2 ** attempt)

    with lock:
        failed += 1
        print(f"  FAILED {username} after {RETRIES} retries")
    return None


# ── Load JSON ────────────────────────────────────────────────────────────────
with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

# Find entries missing source_url
missing = []
for i, row in enumerate(data):
    if "source_url" not in row or not str(row.get("source_url") or "").strip():
        profile = row.get("profile_url", "")
        username = profile.rstrip("/").split("/")[-1] if profile else ""
        if username:
            missing.append((i, username))

print(f"Total entries: {len(data)}")
print(f"Missing source_url: {len(missing)}")
print()

# ── Fetch source_url in parallel ─────────────────────────────────────────────
results = {}  # index -> source_url


def process(item):
    idx, username = item
    url = get_source_url(username)
    if url:
        results[idx] = url


with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [executor.submit(process, item) for item in missing]
    for future in as_completed(futures):
        future.result()

# ── Update JSON ──────────────────────────────────────────────────────────────
for idx, url in results.items():
    data[idx]["source_url"] = url

with open(JSON_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"\nUpdated {JSON_FILE}: {filled} filled, {failed} failed")

# ── Update CSV ───────────────────────────────────────────────────────────────
fieldnames = [
    "first_name", "last_name", "network", "phone_number", "city",
    "postal_code", "profile_url", "number_of_listings", "number_of_reviews",
    "average_rating", "rsac_number", "email_address", "source_url"
]

with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        csv_row = {}
        for k in fieldnames:
            val = row.get(k, "")
            if isinstance(val, list):
                val = ", ".join(str(v) for v in val)
            csv_row[k] = val
        writer.writerow(csv_row)

print(f"Updated {CSV_FILE}")
print(f"\nDone! Filled: {filled} | Failed: {failed}")
