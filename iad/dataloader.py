# iad_agents_to_pg.py
import json
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv


def to_text_array(value):
    """Convert a list to a list of strings, serializing dicts/lists to JSON."""
    if not value:
        return []
    return [item if isinstance(item, str) else json.dumps(item, ensure_ascii=False) for item in value]

load_dotenv()  # load variables from .env

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


with open("IAD_agents_enriched.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS IAD_broke (
    id SERIAL PRIMARY KEY,
    local_id INT,
    agent_id INT,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    gender TEXT,
    phone_number TEXT,
    languages TEXT[],
    network TEXT,
    reviews_count INT,
    reviews_rating_avg NUMERIC,
    status_or_sector TEXT,
    city TEXT[],
    postal_code TEXT,
    rsac_number TEXT,
    social_accounts JSONB,
    profile_url TEXT
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO IAD_broke (
        local_id,
        agent_id,
        username,
        first_name,
        last_name,
        full_name,
        gender,
        phone_number,
        languages,
        network,
        reviews_count,
        reviews_rating_avg,
        status_or_sector,
        city,
        postal_code,
        rsac_number,
        social_accounts,
        profile_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("localId"),
        a.get("agentId"),
        a.get("userName"),
        a.get("first_name"),
        a.get("last_name"),
        a.get("full_name"),
        a.get("displayGender"),
        a.get("phone_number"),
        to_text_array(a.get("languages")),
        a.get("network"),
        a.get("reviewsCount"),
        a.get("reviewsRatingAverage"),
        a.get("status_or_sector"),
        to_text_array(a.get("city")),
        a.get("postal_code"),
        a.get("rsac_number"),
        psycopg2.extras.Json(a.get("social_accounts") or []),
        a.get("profile_url")
    ))

conn.commit()
cur.close()
conn.close()

print("IAD agents inserted successfully")