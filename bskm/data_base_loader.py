# bskm_agents_to_pg.py
import json
import psycopg2

DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "salsabel"

with open("bsk_agents_removed_extra_info.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS bskm_agents (
    id SERIAL PRIMARY KEY,
    agent_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    ads_count INT,
    rsac_number TEXT,
    charge_type TEXT,
    address_city TEXT,
    address_zip TEXT,
    rsac_city TEXT,
    rsac_zip TEXT,
    total_reviews INT,
    url TEXT
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO bskm_agents (
        agent_id, first_name, last_name, email, phone, ads_count, rsac_number, charge_type,
        address_city, address_zip, rsac_city, rsac_zip, total_reviews, url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("id"),
        a.get("firstName"),
        a.get("lastName"),
        a.get("email"),
        a.get("phone"),
        a.get("adsCount"),
        a.get("rsacNumber"),
        a.get("chargeType"),
        a.get("addressCity", {}).get("name"),
        a.get("addressCity", {}).get("zipCode"),
        a.get("rsacCity", {}).get("name"),
        a.get("rsacCity", {}).get("zipCode"),
        a.get("metaReviews", {}).get("total_reviews"),
        a.get("url")
    ))

conn.commit()
cur.close()
conn.close()
print("BSKM agents inserted successfully")