import json

with open("preeves_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)
removed_keys = ["pictureUrl", "description", "immodvisorKey",
                "immodvisorId", "immodvisorHash", "activityTypes"]

for item in data:
    alias = item["alias"]
    item["profile_url"] = f"https://www.proprietes-privees.com/negociateur/{alias}"
    for key in removed_keys:
        item.pop(key, None)


with open('preeves_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

