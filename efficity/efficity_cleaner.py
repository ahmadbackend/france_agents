import json

with open("efficity_agents_data.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

for item in data:
    item["network"] = "efficity"
    item["first_name"] = item["name"].split(" ")[0]
    item["last_name"] = item["name"].split(" ")[1]
    item["full_name"] = item["name"]
    item["phone_number"] = item["mobile"]
    item["profile_url"] = item["url"]

    removed_keys =["name", "mobile", "url"]
    for key in removed_keys:
        if key in item:
            del item[key]
with open("efficity_agents_data_cleaned.json", 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
