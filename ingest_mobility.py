import requests
import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"
TFL_API_BASE = "https://api.tfl.gov.uk"

def ingest_mobility():
    print("🚀 Starting Mobility Intelligence (TfL Footfall Proxy)...")
    
    # 1. Setup
    load_dotenv(override=True)
    # app_key = os.getenv('TFL_APP_KEY') # Optional for higher limits
    
    engine = create_engine(DB_PATH)

    # Ensure table exists
    print("🛠️  Verifying Database Schema...")
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mobility_metrics (
                location_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                latitude DECIMAL(10, 6),
                longitude DECIMAL(10, 6),
                annual_footfall INTEGER,
                metric_type VARCHAR(20),
                last_updated DATE DEFAULT CURRENT_DATE
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mobility_lat ON mobility_metrics(latitude)"))

    # 2. Fetch High-Traffic Nodes (Tube Stations)
    # We query TfL for "Metro" modes (Tube, DLR, Overground)
    print("📡 Querying TfL API for Transport Nodes...")
    
    modes = ["tube", "dlr", "overground"]
    
    total_nodes = 0
    
    for mode in modes:
        try:
            url = f"{TFL_API_BASE}/StopPoint/Mode/{mode}"
            response = requests.get(url)
            
            if response.status_code != 200:
                print(f"   ⚠️ TfL API Error ({mode}): {response.status_code}")
                continue
                
            data = response.json()
            
            # TfL returns a wrapper with 'stopPoints'
            stop_points = data.get('stopPoints', [])
            if not stop_points:
                # Sometimes it returns a list directly depending on endpoint
                if isinstance(data, list): stop_points = data
            
            print(f"   Found {len(stop_points)} {mode.upper()} stations.")
            
            for station in stop_points:
                # Extract Data
                station_id = station.get('id')
                name = station.get('commonName')
                lat = station.get('lat')
                lon = station.get('lon')
                
                # Footfall Proxy:
                # The StopPoint API doesn't give annual exit counts directly in this payload.
                # We simulate a 'Vibrancy Score' based on lines serving the station.
                # Real annual counts come from a separate static CSV usually.
                # For MVP, we assign a 'Base Vibrancy' of 1M, boosted by line count.
                
                lines = station.get('lines', [])
                vibrancy_score = 1000000 + (len(lines) * 500000) # Mock logic for visualization
                
                with engine.connect() as conn:
                    conn.execute(text("""
                        INSERT OR REPLACE INTO mobility_metrics 
                        (location_id, name, latitude, longitude, annual_footfall, metric_type)
                        VALUES (:id, :name, :lat, :lon, :score, :type)
                    """), {
                        "id": station_id,
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "score": vibrancy_score,
                        "type": f"TFL_{mode.upper()}"
                    })
                    conn.commit()
                    total_nodes += 1
                    
        except Exception as e:
            print(f"   ❌ API Exception: {e}")
            
    print("=========================================")
    print(f"🎉 MOBILITY INDEX COMPLETE.")
    print(f"📊 Total Transport Nodes Mapped: {total_nodes}")
    print("=========================================")

if __name__ == "__main__":
    ingest_mobility()
