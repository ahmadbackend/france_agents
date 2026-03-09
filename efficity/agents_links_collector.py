from patchright.sync_api import sync_playwright
from urllib.parse import urljoin
import json
from bs4 import BeautifulSoup
import re
from curl_cffi import requests
from bs4 import BeautifulSoup as bs
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

URL = "https://www.efficity.com/consultants-immobiliers/liste/?gad_source=1&gad_campaignid=20714983801&gbraid=0AAAAAD8I0F4W9nMxxStyqeIRSdHT1pcwa&gclid=Cj0KCQjw37nNBhDkARIsAEBGI8MLgK3dUtFFPv2aJ6M-rIZokzjpaYUcdtEllw1ByVeeaOpMAWtsK1kaAtzPEALw_wcB"

headers = {
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
    "referer": "https://www.efficity.com/",
}

def agents_links():
    session = requests.Session(impersonate="chrome")
    r = session.get(URL, headers=headers)
    html_content = r.text
    soup = bs(html_content, "html.parser")
    agent_links = []

    for a in soup.select("div.index-list-item a[href]"):
        agent_links.append(urljoin("https://www.efficity.com", a["href"]))

    with open("efficity_agents_links.json", "w", encoding="utf-8") as f:
        json.dump(agent_links, f, ensure_ascii=False, indent=2)



def scrape_agents(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as f:
        urls = json.load(f)
    agents = []

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)

            print(f"Status: {r.status_code} -> {url}")

            soup = BeautifulSoup(r.text, "html.parser")

            name = None
            mobile = None
            email = None
            location = None
            rating = None
            reviews_count = None

            scripts = soup.find_all("script", type="application/ld+json")

            for s in scripts:
                try:
                    data = json.loads(s.string)

                    # sometimes schema contains lists
                    if isinstance(data, list):
                        objects = data
                    else:
                        objects = [data]

                    for obj in objects:

                        # ---------- AGENT INFO ----------
                        if obj.get("@type") == "RealEstateAgent":

                            name = obj.get("name")
                            mobile = obj.get("telephone")
                            email = obj.get("email")

                            address = obj.get("address", {})
                            city = address.get("addressLocality")
                            region = address.get("addressRegion")

                            if city and region:
                                location = f"{city} ({region})"
                            else:
                                location = city

                        # ---------- RATING ----------
                        if obj.get("@type") == "Product":

                            rating_data = obj.get("aggregateRating")

                            if rating_data:
                                rating = rating_data.get("ratingValue")
                                reviews_count = rating_data.get("reviewCount")

                except Exception:
                    continue

            agents.append({
                "url": url,
                "name": name,
                "location": location,
                "mobile": mobile,
                "email": email,
                "rating": rating,
                "reviews_count": reviews_count
            })
            print(agents)

        except Exception as e:
            print(f"failed: {url} -> {e}")


    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(agents, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    agents_links()
    scrape_agents(input_file="efficity_agents_links.json", output_file="efficity_agents_data.json")

