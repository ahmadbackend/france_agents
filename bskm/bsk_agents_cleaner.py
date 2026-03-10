import json

with open("bsk_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

for item in data:
    if "addressCity" in item and isinstance(item["addressCity"], dict):
        item["addressCity"].pop("html", None)  # Remove html key safely

    if "rsacCity" in item and isinstance(item["rsacCity"], dict):
        item["rsacCity"].pop("html", None)  # Remove html key
    total_reviews = item["metaReviews"].get("total", 0)
    item["network"] = "bskimmobilier"
    item["first_name"] = item["firstName"]
    item["last_name"] = item["lastName"]
    item["full_name"] = f"{item['firstName']} {item['lastName']}"
    item["phone_number"] = item["phone"]
    item["profile_url"] = item["url"]

    # Replace the whole metaReviews object with just the total
    item["metaReviews"] = {"total_reviews": total_reviews}
    address_city, address_code = item.get("addressCity", {}).get("name"), item.get("addressCity", {}).get("zipCode")
    rsac_city, rsac_code = item.get("rsacCity", {}).get("name"), item.get("rsacCity", {}).get("zipCode")

    item["city"] = [item.get("addressCity", {}).get("name"), item.get("rsacCity", {}).get("name")
                    ]if address_city!=rsac_city else [address_city]
    item["postal_code"] = [item.get("addressCity", {}).get("zipCode"),
                           item.get("rsacCity", {}).get("zipCode")] if address_code!=rsac_code else [address_code]


    for key in ["wantsToDisplayHisTeam", "wantsToDisplayItsLatestSales", "wantsToHideItsLatestSalesPrice",
                "photo", "sales", "html", "description", "lastName", "firstName", "phone", "url", "addressCity", "rsacCity"
                ]:
        item.pop(key, None)

with open('bsk_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Done! Updated {len(data)} agents.")