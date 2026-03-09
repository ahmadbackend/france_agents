# preeves_agents_to_pg.py
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # load variables from .env

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

with open("preeves_agents.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS preeves_agents (
    id SERIAL PRIMARY KEY,
    agent_id TEXT,
    active BOOLEAN,
    first_name TEXT,
    last_name TEXT,
    phone TEXT,
    zone TEXT,
    identifier TEXT,
    type TEXT,
    civility TEXT,
    google_my_business TEXT,
    facebook_url TEXT,
    alias TEXT,
    place_label TEXT,
    place_code TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    parent_place_label TEXT,
    profile_url TEXT
)
""")

for a in agents:
    location = a.get("location", {})
    parent = location.get("parent", {})
    cur.execute("""
    INSERT INTO preeves_agents (
        agent_id, active, first_name, last_name, phone, zone, identifier, type, civility,
        google_my_business, facebook_url, alias,
        place_label, place_code, latitude, longitude, parent_place_label, profile_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("id"),
        a.get("active"),
        a.get("firstname"),
        a.get("lastname"),
        a.get("phone"),
        a.get("zone"),
        a.get("identifier"),
        a.get("type"),
        a.get("civility"),
        a.get("googleMyBusinessUrl"),
        a.get("facebookUrl"),
        a.get("alias"),
        location.get("label"),
        location.get("code"),
        location.get("latitude"),
        location.get("longitude"),
        parent.get("label"),
        a.get("profile_url")
    ))

conn.commit()
cur.close()
conn.close()
print("Preeves agents inserted successfully")