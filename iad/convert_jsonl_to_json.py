import json
import sys
from pathlib import Path


def jsonl_to_json(input_path: str = "IAD_agents_enriched.jsonl", output_path: str = "IAD_agents_enriched.json"):
    input_file = Path(input_path)

    if not input_file.exists():
        print(f"Error: File '{input_path}' not found.")
        sys.exit(1)

    if not input_file.suffix == ".jsonl":
        print("Warning: File does not have a .jsonl extension.")

    if output_path is None:
        output_path = input_file.with_suffix(".json")

    records = []
    errors = 0

    with open(input_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
                errors += 1

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Done! Converted {len(records)} records to '{output_path}'.")
    if errors:
        print(f"Skipped {errors} invalid line(s).")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python jsonl_to_json.py <input.jsonl> [output.json]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    jsonl_to_json(input_path, output_path)