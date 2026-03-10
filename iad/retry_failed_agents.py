import json
import time
from curl_cffi import requests

FAILED_FILE = "IAD_failed.txt"
OUTPUT_FILE = "IAD_agents_enriched.jsonl"
FAILED_AGAIN_FILE = "IAD_failed_again.txt"

BASE_URL = "https://www.iadfrance.fr/api/agents/{}?locale=fr"
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

RETRIES = 3

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.iadfrance.fr/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def fetch_agent(username):

    url = BASE_URL.format(username)

    for attempt in range(RETRIES):

        try:
            r = requests.get(
                url,
                headers=headers,
                impersonate="chrome",
                proxy=PROXY,
                timeout=30
            )

            if r.status_code == 200:
                return r.json()

        except Exception as e:
            print(f"Retry {attempt+1} for {username} -> {e}")

        time.sleep(2)

    return None


with open(FAILED_FILE, "r", encoding="utf-8") as f:
    usernames = [x.strip() for x in f if x.strip()]

print("Total failed to retry:", len(usernames))


for i, username in enumerate(usernames, 1):

    print(f"{i}/{len(usernames)} -> {username}")

    data = fetch_agent(username)

    if not data:

        with open(FAILED_AGAIN_FILE, "a", encoding="utf-8") as f:
            f.write(username + "\n")

        continue

    location = data.get("location") or {}
    legal = data.get("legal") or {}
    rsac = legal.get("rsac") or {}

    loc_place = location.get("place")
    rsac_place = rsac.get("place")

    if loc_place and rsac_place and loc_place != rsac_place:
        city = [loc_place, rsac_place]
    elif loc_place:
        city = [loc_place]
    else:
        city = []

    result = {
        "userName": username,
        "city": city,
        "postal_code": location.get("postcode"),
        "rsac_number": rsac.get("number"),
        "social_accounts": data.get("socialNetworks", [])
    }

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")

    time.sleep(0.4)


print("Retry finished.")