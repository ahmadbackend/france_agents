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
CREATE TABLE IF NOT EXISTS bskim_mobilier_agents (
    id SERIAL PRIMARY KEY,
    agent_id INT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    email TEXT,
    phone_number TEXT,
    ads_count INT,
    rsac_number TEXT,
    charge_type TEXT,
    city TEXT[],
    postal_code TEXT[],
    total_reviews INT,
    profile_url TEXT,
    network TEXT    
)
""")

for a in agents:
    cur.execute("""
    INSERT INTO bskim_mobilier_agents (
        agent_id,
        first_name,
        last_name,
        full_name,
        email,
        phone_number,
        ads_count,
        rsac_number,
        charge_type,
        city,
        postal_code,
        total_reviews,
        profile_url,
        network
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)
    """, (
        a.get("id"),
        a.get("first_name"),
        a.get("last_name"),
        a.get("full_name"),
        a.get("email"),
        a.get("phone_number"),
        a.get("adsCount"),
        a.get("rsacNumber"),
        a.get("chargeType"),
        a.get("city", []),
        a.get("postal_code", []),
        a.get("metaReviews", {}).get("total_reviews"),
        a.get("profile_url"),
        a.get("network")
    ))

conn.commit()
cur.close()
conn.close()
print("BSKM agents inserted successfully")