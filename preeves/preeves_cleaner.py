import json

with open("preeves_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

for item in data:
    alias = item["alias"]
    item["profile_url"] = f"https://www.proprietes-privees.com/negociateur/{alias}"
    item["first_name"] = item["firstname"]
    item["last_name"] = item["lastname"]
    item["full_name"] = item["firstname"] + " " + item["lastname"]
    item["phone_number"] = item["phone"]
    item["network"] = "preeves"
    zone = item.get("zone", "")
    city = item["location"].get("label", "")
    item["city"] = [zone, city] if zone and city and zone != city else [zone or city]
    item["postal_code"] = item.get("location", {}).get("code", "")
    removed_keys = ["alias", "firstname", "lastname", "phone", "zone", "location", "immodvisorKey",
                    "immodvisorId", "immodvisorHash", "activityTypes", "description", "location", "pictureUrl"]

    for key in removed_keys:
        item.pop(key, None)


with open('preeves_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

