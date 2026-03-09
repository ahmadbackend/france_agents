import json
import psycopg2
import os
from dotenv import load_dotenv

# ---------------- Load .env ----------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ---------------- Load JSON ----------------
with open("saftri_agents_removed_extra_info.json", "r", encoding="utf-8") as f:
    agents = json.load(f)

# ---------------- Connect to PostgreSQL ----------------
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

# ---------------- Create table ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS safti_agents (
    id SERIAL PRIMARY KEY,
    last_name TEXT,
    first_name TEXT,
    post_code TEXT,
    city TEXT,
    phone_number TEXT,
    principal_area TEXT,
    other_areas TEXT,
    nb_properties INT,
    lat NUMERIC,
    lng NUMERIC,
    civility TEXT,
    slug TEXT,
    google_mb TEXT,
    google_mb_name TEXT,
    google_mb_user_ratings_total INT,
    google_mb_url TEXT,
    google_mb_rating NUMERIC,
    profile_url TEXT
)
""")

# ---------------- Insert data ----------------
for a in agents:
    cur.execute("""
    INSERT INTO safti_agents (
        last_name, first_name, post_code, city, phone_number,
        principal_area, other_areas, nb_properties, lat, lng,
        civility, slug, google_mb, google_mb_name, google_mb_user_ratings_total,
        google_mb_url, google_mb_rating, profile_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("lastName"),
        a.get("firstName"),
        a.get("postCode"),
        a.get("city"),
        a.get("phoneNumber"),
        a.get("principalArea"),
        a.get("otherAreas"),
        a.get("nbProperties"),
        a.get("lat"),
        a.get("lng"),
        a.get("civility"),
        a.get("slug"),
        a.get("googleMB"),
        a.get("googleMBName"),
        a.get("googleMBUserRatingsTotal"),
        a.get("googleMBUrl"),
        a.get("googleMBRating"),
        a.get("profile_url")
    ))

# ---------------- Commit and close ----------------
conn.commit()
cur.close()
conn.close()

print("Safti agents inserted successfully")