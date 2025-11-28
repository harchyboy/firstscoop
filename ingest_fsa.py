import requests
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"
FSA_API_BASE = "http://api.ratings.food.gov.uk"
HEADERS = {'x-api-version': '2'} # FSA requires this header

def ingest_fsa():
    print("🚀 Starting FSA Hygiene Ingestion (Ghost Town Signal)...")
    
    # 1. Setup
    load_dotenv(override=True)
    engine = create_engine(DB_PATH)

    # Ensure table exists
    print("🛠️  Verifying Database Schema...")
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fsa_ratings (
                fsa_id INTEGER PRIMARY KEY,
                business_name TEXT,
                address_line_1 TEXT,
                postcode VARCHAR(10),
                rating_value VARCHAR(10),
                rating_date DATE,
                latitude DECIMAL(10, 6),
                longitude DECIMAL(10, 6),
                local_authority_code VARCHAR(10)
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fsa_date ON fsa_ratings(rating_date)"))

    # 2. Fetch Authorities (to iterate properly)
    # For MVP, we target key London boroughs to find 'Zombie High Streets'
    # Tower Hamlets = 530, Hackney = 517, Newham = 527
    target_authorities = [530, 517, 527]
    
    total_ingested = 0
    
    for auth_id in target_authorities:
        print(f"📡 Querying FSA Authority ID: {auth_id}...")
        
        try:
            # Iterate pages
            page = 1
            while True:
                url = f"{FSA_API_BASE}/Establishments?localAuthorityId={auth_id}&pageSize=100&pageNumber={page}"
                response = requests.get(url, headers=HEADERS)
                
                if response.status_code != 200:
                    print(f"   ⚠️ API Error: {response.status_code}")
                    break
                    
                data = response.json()
                establishments = data.get('establishments', [])
                
                if not establishments:
                    break # End of pages
                
                count_in_batch = 0
                with engine.connect() as conn:
                    for est in establishments:
                        # Extract fields
                        fsa_id = est.get('FHRSID')
                        name = est.get('BusinessName')
                        addr = est.get('AddressLine1')
                        postcode = est.get('PostCode')
                        rating = est.get('RatingValue')
                        date_str = est.get('RatingDate')
                        
                        # Parse Date
                        # FSA date format: '2023-10-12T00:00:00'
                        rating_date = None
                        if date_str:
                            try:
                                rating_date = date_str.split('T')[0]
                            except:
                                pass
                        
                        # Geocode
                        lat = None
                        lng = None
                        geocode = est.get('geocode')
                        if geocode:
                            lat = geocode.get('latitude')
                            lng = geocode.get('longitude')
                            
                        conn.execute(text("""
                            INSERT OR REPLACE INTO fsa_ratings 
                            (fsa_id, business_name, address_line_1, postcode, rating_value, rating_date, latitude, longitude, local_authority_code)
                            VALUES (:id, :name, :addr, :pc, :rating, :date, :lat, :lng, :auth)
                        """), {
                            "id": fsa_id,
                            "name": name,
                            "addr": addr,
                            "pc": postcode,
                            "rating": rating,
                            "date": rating_date,
                            "lat": lat,
                            "lng": lng,
                            "auth": str(auth_id)
                        })
                        count_in_batch += 1
                    conn.commit()
                
                total_ingested += count_in_batch
                # print(f"   Page {page}: Ingested {count_in_batch} units.")
                page += 1
                
                if page > 5: break # Safety limit for MVP demo speed
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            
    print("=========================================")
    print(f"🎉 FSA SCAN COMPLETE.")
    print(f"📊 Total Retail Units Mapped: {total_ingested}")
    print("=========================================")

if __name__ == "__main__":
    ingest_fsa()
