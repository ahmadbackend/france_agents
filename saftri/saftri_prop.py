import json
import math
import time
from curl_cffi import requests

URL = "https://api.safti.fr/public_site/agent/search"

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar-SA;q=0.8,ar;q=0.7",
    "content-type": "application/json",
    "origin": "https://www.safti.fr",
    "referer": "https://www.safti.fr/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

LIMIT = 9

session = requests.Session(impersonate="chrome")

all_agents = []
failed_pages = []

def fetch_page(page):
    payload = {
        "page": page,
        "limit": LIMIT,
        "seed": 0
    }

    try:
        r = session.post(URL, headers=headers, json=payload, timeout=30)

        if r.status_code == 403:
            print(f"403 Forbidden on page {page}")
            failed_pages.append({"page": page, "status": 403})
            return None

        if r.status_code != 200:
            print(f"Error {r.status_code} on page {page}")
            failed_pages.append({"page": page, "status": r.status_code})
            return None

        return r.json()

    except Exception as e:
        print(f"Exception on page {page}: {e}")
        failed_pages.append({"page": page, "error": str(e)})
        return None


print("Fetching first page to determine total pages...")
first = fetch_page(1)

if not first:
    raise Exception("Failed to fetch first page")

total_count = first["totalCount"]
total_pages = math.ceil(total_count / LIMIT)

print(f"Total agents: {total_count}")
print(f"Total pages: {total_pages}")

all_agents.extend(first["agents"])

for page in range(1, total_pages + 1):

    print(f"Scraping page {page}/{total_pages}")

    data = fetch_page(page)

    if data and "agents" in data:
        all_agents.extend(data["agents"])

    time.sleep(0.5)


with open("saftri_agents.json", "w", encoding="utf-8") as f:
    json.dump(all_agents, f, ensure_ascii=False, indent=2)


with open("error_pages.txt", "w") as f:
    for e in failed_pages:
        f.write(json.dumps(e) + "\n")


print(f"Saved {len(all_agents)} agents")
print(f"Failed pages: {len(failed_pages)}")