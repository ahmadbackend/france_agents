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
CREATE TABLE IF NOT EXISTS preeves_brokers (
    id SERIAL PRIMARY KEY,
    agent_id TEXT,
    active BOOLEAN,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    phone TEXT,
    network TEXT,
    identifier TEXT,
    type TEXT,
    civility TEXT,
    google_my_business TEXT,
    facebook_url TEXT,
    alias TEXT,
    city TEXT[],
    postal_code TEXT,
    profile_url TEXT
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO preeves_brokers (
        agent_id, active, first_name, last_name, full_name, phone, network,
        identifier, type, civility,
        google_my_business, facebook_url, alias,
        city, postal_code, profile_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("id"),
        a.get("active"),
        a.get("first_name"),
        a.get("last_name"),
        a.get("full_name"),
        a.get("phone_number"),
        a.get("network"),
        a.get("identifier"),
        a.get("type"),
        a.get("civility"),
        a.get("googleMyBusinessUrl"),
        a.get("facebookUrl"),
        a.get("alias"),
        a.get("city"),
        a.get("postal_code"),
        a.get("profile_url")
    ))

conn.commit()
cur.close()
conn.close()
print("Preeves agents inserted successfully")