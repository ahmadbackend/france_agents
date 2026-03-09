# efficity_agents_to_pg.py
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # load variables from .env

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

with open("efficity_agents_data.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS efficity_agents (
    id SERIAL PRIMARY KEY,
    url TEXT,
    name TEXT,
    location TEXT,
    mobile TEXT,
    email TEXT,
    rating NUMERIC,
    reviews_count INT
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO efficity_agents (url, name, location, mobile, email, rating, reviews_count)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("url"),
        a.get("name"),
        a.get("location"),
        a.get("mobile"),
        a.get("email"),
        a.get("rating"),
        a.get("reviews_count")
    ))

conn.commit()
cur.close()
conn.close()
print("Efficity agents inserted successfully")