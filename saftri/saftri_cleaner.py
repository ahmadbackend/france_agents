import json

with open("saftri_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)
removed_keys = ["photo", "photoConseillerMinisite", "googleMBShow", "whatsAppShow", "codePrescripteur",
                "uuid", "firstName", "lastName", "otherAreas", "phoneNumber", "location", "absence",
                "otherAreas", "principalArea", "lat", "lng"]

for item in data:
    slug = item["slug"]
    item["profile_url"] = f"https://www.safti.fr/votre-conseiller-safti/{slug}"
    item["first_name"] = item["firstName"]
    item["last_name"] = item["lastName"]
    item["full_name"] = item["first_name"] + " " + item["last_name"]
    item["network"] = "SAFTI"
    city = item.get("city", "")
    otherAreas = item.get("otherAreas", []).split(",") if item.get("otherAreas") else []
    item["city"] = [city] + otherAreas if city else otherAreas
    item["phone_number"] = item.get("phoneNumber", "")

    for key in removed_keys:
        item.pop(key, None)


with open('saftri_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

