import os
import requests
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"
PLANIT_API_BASE = "https://www.planit.org.uk/api/applics/json"

def ingest_planning():
    print("🚀 Starting Planning Intelligence (PlanIt API)...")
    
    # 1. Setup
    load_dotenv(override=True)
    # api_key = os.getenv('PLANIT_API_KEY') # If needed for commercial use
    
    engine = create_engine(DB_PATH)

    # Ensure table exists
    print("🛠️  Verifying Database Schema...")
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS planning_history (
                application_id VARCHAR(50) PRIMARY KEY,
                uprn VARCHAR(20),
                address TEXT,
                description TEXT,
                status VARCHAR(50),
                decision_date DATE,
                expiry_date DATE,
                is_lapsing_soon BOOLEAN DEFAULT 0,
                url TEXT
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_planning_status ON planning_history(status)"))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_planning_expiry ON planning_history(expiry_date)"))

    # 2. Define "Snipe Window" (Permission Granted 2.5 - 3.0 Years Ago)
    today = datetime.now()
    window_start = today - timedelta(days=365 * 3)      # 3 years ago
    window_end = today - timedelta(days=365 * 2.5)      # 2.5 years ago
    
    print(f"🎯 Snipe Window: Searching for permissions granted between {window_start.strftime('%Y-%m-%d')} and {window_end.strftime('%Y-%m-%d')}")

    # 3. Target Selection
    # We only query for assets we already track (Distressed) to save API calls
    with engine.connect() as conn:
        query_targets = text("""
            SELECT uprn, postcode, latitude, longitude 
            FROM master_properties p
            JOIN epc_assessments e ON p.uprn = e.uprn
            WHERE e.asset_rating_band IN ('E', 'F', 'G')
              AND p.latitude IS NOT NULL
            LIMIT 50
        """)
        targets = conn.execute(query_targets).fetchall()

    print(f"📡 Scanning planning history for {len(targets)} distressed assets...")

    for target in targets:
        uprn, postcode, lat, lng = target
        
        # 4. Query PlanIt API (Spatial Search)
        # Note: PlanIt allows lat/lon queries
        params = {
            'lat': lat,
            'lng': lng,
            'kml_type': 'p', # Points
            'limit': 10,
            'recent': 30, # Days? No, we need history. PlanIt API params vary by subscription.
            # For 'Lapse' check, we need historical data (Start Date parameter)
            'start_date': window_start.strftime('%Y-%m-%d'),
            'end_date': window_end.strftime('%Y-%m-%d')
        }
        
        try:
            # Example GET request (Mocked loop since we don't want to spam without key)
            # response = requests.get(PLANIT_API_BASE, params=params)
            # data = response.json()
            
            # --- MOCK RESPONSE FOR DEMO ARCHITECTURE ---
            data = {'records': []} 
            # If we found a record:
            # record = {
            #    'uid': 'PA/22/01234',
            #    'description': 'Demolition and erection of 4 storey block',
            #    'status': 'Granted',
            #    'date_decision': '2022-06-15'
            # }
            
            for record in data.get('records', []):
                # Logic: Check Status
                status = record.get('status', 'Unknown')
                
                if status == 'Granted':
                    decision_date = datetime.strptime(record['date_decision'], '%Y-%m-%d')
                    expiry_date = decision_date + timedelta(days=365*3)
                    days_remaining = (expiry_date - today).days
                    
                    is_lapsing = 0 < days_remaining < 180 # Less than 6 months left
                    
                    print(f"   ⚠️ FOUND GRANTED PERMISSION: {record['uid']}")
                    print(f"      Expires: {expiry_date.strftime('%Y-%m-%d')} ({days_remaining} days left)")
                    
                    # Save to DB
                    with engine.connect() as conn:
                        conn.execute(text("""
                            INSERT OR REPLACE INTO planning_history 
                            (application_id, uprn, address, description, status, decision_date, expiry_date, is_lapsing_soon, url)
                            VALUES (:id, :uprn, :addr, :desc, :status, :dec_date, :exp_date, :lapsing, :url)
                        """), {
                            "id": record['uid'],
                            "uprn": uprn,
                            "addr": record.get('address'),
                            "desc": record.get('description'),
                            "status": status,
                            "dec_date": record['date_decision'],
                            "exp_date": expiry_date,
                            "lapsing": is_lapsing,
                            "url": record.get('url')
                        })
                        conn.commit()

        except Exception as e:
            print(f"   ❌ API Error: {e}")
            
        time.sleep(0.2) # Respect rate limits

    print("=========================================")
    print(f"🎉 PLANNING SCAN COMPLETE.")
    print("=========================================")

if __name__ == "__main__":
    ingest_planning()
