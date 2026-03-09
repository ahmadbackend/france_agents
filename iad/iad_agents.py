import json
import base64
import time
from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

INPUT_FILE = "iad_locations.json"
OUTPUT_FILE = "ia_agents.json"

headers = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

session = requests.Session()


def decode_phone(hashed):
    try:
        return base64.b64decode(hashed).decode()
    except:
        return None


def fetch_sector(slug, retries=5):

    url = f"https://www.iadfrance.fr/api/agents/sector/{slug}?locale=fr"

    for attempt in range(retries):
        try:

            r = session.get(
                url,
                headers=headers,
                impersonate="chrome",
                timeout=30,
                proxy=PROXY,
            )

            if r.status_code == 200:
                return r.json().get("items", [])

        except RequestException as e:
            print(f"Retry {attempt+1}/{retries} for {slug} : {e}")

        time.sleep(2)

    print("FAILED:", slug)
    return []


def main():

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        locations = json.load(f)

    seen_agents = set()
    all_agents = []

    for i, loc in enumerate(locations):

        slug = loc["slug"]

        print(f"{i+1}/{len(locations)} -> {slug}")

        agents = fetch_sector(slug)

        for agent in agents:

            agent_id = agent.get("agentId")

            if agent_id in seen_agents:
                continue

            seen_agents.add(agent_id)

            hashed = agent.get("directContact", {}).get("hashedPhone")
            phone = decode_phone(hashed) if hashed else None
            agent["phone"] = phone
            all_agents.append(agent)

        # save progress every 100 requests
        if i % 100 == 0:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(all_agents, f, ensure_ascii=False, indent=2)

        time.sleep(0.25)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_agents, f, ensure_ascii=False, indent=2)

    print("Saved", len(all_agents), "unique agents")


if __name__ == "__main__":
    main()