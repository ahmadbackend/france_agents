import json
import time
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

INPUT_FILE = "IAD_agents_cleaned.json"
OUTPUT_FILE = "IAD_agents_enriched.jsonl"
FAILED_FILE = "IAD_failed.txt"

BASE_URL = "https://www.iadfrance.fr/api/agents/{}?locale=fr"
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

MAX_THREADS = 10
RETRIES = 3

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.iadfrance.fr/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

write_lock = Lock()


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

        except Exception:
            pass

        time.sleep(1)

    return None


def process_agent(agent):

    username = agent.get("userName")

    if not username:
        return None

    data = fetch_agent(username)

    if not data:
        with write_lock:
            with open(FAILED_FILE, "a", encoding="utf-8") as f:
                f.write(username + "\n")
        return None

    location = data.get("location") or {}
    legal = data.get("legal") or {}
    rsac = legal.get("rsac") or {}

    loc_place = location.get("place")
    rsac_place = rsac.get("place")

    if loc_place and rsac_place and loc_place != rsac_place:
        agent["city"] = [loc_place, rsac_place]
    elif loc_place:
        agent["city"] = [loc_place]
    else:
        agent["city"] = []

    agent["postal_code"] = location.get("postcode")
    agent["rsac_number"] = rsac.get("number")
    agent["social_accounts"] = data.get("socialNetworks", [])

    return agent


def save_result(agent):

    with write_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(agent, ensure_ascii=False) + "\n")


def main():

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        agents = json.load(f)

    total = len(agents)
    print(f"Total agents: {total}")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = {executor.submit(process_agent, agent): agent for agent in agents}

        for i, future in enumerate(as_completed(futures), 1):

            result = future.result()

            if result:
                save_result(result)

            if i % 100 == 0:
                print(f"Processed {i}/{total}")


if __name__ == "__main__":
    main()