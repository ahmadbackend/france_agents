import json

with open("saftri_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)
removed_keys = ["photo", "photoConseillerMinisite", "googleMBShow", "whatsAppShow", "codePrescripteur", "uuid"]

for item in data:
    slug = item["slug"]
    item["profile_url"] = f"https://www.safti.fr/votre-conseiller-safti/{slug}"
    for key in removed_keys:
        item.pop(key, None)


with open('saftri_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

