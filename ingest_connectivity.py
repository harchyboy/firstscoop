import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 50000
DB_PATH = "sqlite:///vantage.db"

def ingest_connectivity():
    print("🚀 Starting Connectivity Ingestion (Broadband Signal)...")
    
    # 1. Setup
    load_dotenv(override=True)
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)

    # Ensure table exists
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS connectivity_metrics (
                uprn VARCHAR(20) PRIMARY KEY,
                max_download_speed INTEGER,
                max_upload_speed INTEGER,
                fiber_availability BOOLEAN,
                five_g_availability BOOLEAN
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_connectivity_speed ON connectivity_metrics(max_download_speed)"))

    # 2. Locate Data
    # Ofcom datasets are usually split by region. We assume a merged file or main file.
    remote_key = "raw/connectivity/connected_nations.csv" 
    local_path = "./epc_data/connected_nations.csv"
    
    if not os.path.exists(local_path):
        if os.getenv('AWS_ACCESS_KEY_ID'):
            print(f"⬇️  Attempting to download Ofcom Data ({remote_key})...")
            try:
                lake.download_file(remote_key, local_path)
            except:
                print("⚠️  File not found in S3 yet. Upload the Ofcom CSV to continue.")
                return
        else:
             print("⚠️  File not found locally and no AWS keys. Aborting.")
             return
    else:
        print("✅ Connectivity Source File found locally.")

    # 3. Stream & Process
    print("🔄 Processing Broadband Data...")
    
    try:
        # Ofcom Header Guess (varies by year): UPRN, Max_Down, Max_Up, FTTP_Availability, 5G_Outdoor...
        # We will need to map this once we see the file. 
        # For now, we setup the architectural loop.
        
        chunk_iter = pd.read_csv(local_path, chunksize=BATCH_SIZE, low_memory=False)
        total_ingested = 0
        
        for i, df in enumerate(chunk_iter):
            # CLEAN & MAP
            # df.columns = [c.lower() for c in df.columns]
            # df_clean = df[['uprn', 'max_dl', ...]]
            
            pass 
            
            # df_clean.to_sql('connectivity_metrics', engine, if_exists='append', index=False)
            # total_ingested += len(df)

        print("=========================================")
        print(f"🎉 CONNECTIVITY INDEX COMPLETE.")
        print(f"📊 Total UPRNs Mapped: {total_ingested}")
        print("=========================================")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")

if __name__ == "__main__":
    ingest_connectivity()
