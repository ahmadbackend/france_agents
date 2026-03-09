import json

with open("bsk_agents.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

for item in data:
    for key in ["wantsToDisplayHisTeam", "wantsToDisplayItsLatestSales", "wantsToHideItsLatestSalesPrice",
                "photo", "sales", "html", "description"
                ]:
        item.pop(key, None)
    if "addressCity" in item and isinstance(item["addressCity"], dict):
        item["addressCity"].pop("html", None)  # Remove html key safely

    if "rsacCity" in item and isinstance(item["rsacCity"], dict):
        item["rsacCity"].pop("html", None)  # Remove html key
    total_reviews = item["metaReviews"].get("total", 0)
    # Replace the whole metaReviews object with just the total
    item["metaReviews"] = {"total_reviews": total_reviews}

with open('bsk_agents_removed_extra_info.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Done! Updated {len(data)} agents.")