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
CREATE TABLE IF NOT EXISTS safti_brokers (
    id SERIAL PRIMARY KEY,
    network TEXT,
    last_name TEXT,
    first_name TEXT,
    full_name TEXT,
    post_code TEXT,
    city TEXT[],
    phone_number TEXT,
    nb_properties INT,
    civility TEXT,
    slug TEXT,
    google_mb TEXT,
    google_mb_name TEXT,
    google_mb_user_ratings_total INT,
    google_mb_url TEXT,
    google_mb_rating NUMERIC,
    annee_club_developpeur INT,
    conseiller_club_developpeur BOOLEAN,
    profile_url TEXT
)
""")

# ---------------- Insert data ----------------
for a in agents:
    cur.execute("""
    INSERT INTO safti_brokers (
        network, last_name, first_name, full_name, post_code, city, phone_number,
        nb_properties, civility, slug,
        google_mb, google_mb_name, google_mb_user_ratings_total,
        google_mb_url, google_mb_rating,
        annee_club_developpeur, conseiller_club_developpeur,
        profile_url
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        a.get("network"),
        a.get("last_name"),
        a.get("first_name"),
        a.get("full_name"),
        a.get("postCode"),
        a.get("city"),
        a.get("phone_number"),
        a.get("nbProperties"),
        a.get("civility"),
        a.get("slug"),
        a.get("googleMB"),
        a.get("googleMBName"),
        a.get("googleMBUserRatingsTotal"),
        a.get("googleMBUrl"),
        a.get("googleMBRating"),
        a.get("anneeClubDeveloppeur"),
        a.get("conseillerClubDeveloppeur"),
        a.get("profile_url")
    ))

# ---------------- Commit and close ----------------
conn.commit()
cur.close()
conn.close()

print("Safti agents inserted successfully")