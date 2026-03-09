# iad_agents_to_pg.py
import json
import psycopg2

import os
from dotenv import load_dotenv

load_dotenv()  # load variables from .env

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

with open("ia_agents.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS iad_agents (
    id SERIAL PRIMARY KEY,
    local_id INT,
    agent_id INT,
    username TEXT,
    full_name TEXT,
    gender TEXT,
    phone TEXT,
    property_count INT,
    reviews_count INT,
    reviews_rating_avg NUMERIC,
    status_or_sector TEXT,
    avatar_url TEXT
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO iad_agents (
        local_id, agent_id, username, full_name, gender, phone,
        property_count, reviews_count, reviews_rating_avg, status_or_sector, avatar_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("localId"),
        a.get("agentId"),
        a.get("userName"),
        a.get("fullName"),
        a.get("displayGender"),
        a.get("phone"),
        a.get("propertyCount"),
        a.get("reviewsCount"),
        a.get("reviewsRatingAverage"),
        a.get("statusOrSector"),
        a.get("avatar", {}).get("src")
    ))

conn.commit()
cur.close()
conn.close()
print("IAD agents inserted successfully")