import json

with open("ia_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

for item in data:
    item["network"] = "IAD"
    item["profile_url"] = f"https://www.iadfrance.fr/conseiller-immobilier/{item['userName']}"
    item["first_name"] = item["fullName"].split()[0] if item["fullName"] else ""
    item["last_name"] = item["fullName"].split()[-1] if item["fullName"] else ""
    item["phone_number"] = item.get("phone", "")
    item["full_name"] = item.get("fullName", "")
    item["status_or_sector"] = item.get("statusOrSector", "")
    for key in [ "phone", "fullName", "statusOrSector", "directContact", "avatar"]:
        item.pop(key, None)

with open("IAD_agents_cleaned.json", 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)