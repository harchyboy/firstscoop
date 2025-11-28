import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv
from datetime import timedelta

# --- CONFIGURATION ---
BATCH_SIZE = 20000
DB_PATH = "sqlite:///vantage.db"

def ingest_leases():
    print("🚀 Starting Registered Leases Ingestion Pipeline...")
    
    # 1. Setup
    load_dotenv(override=True)
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)

    # Ensure table exists
    print("🛠️  Verifying Database Schema...")
    with open("schema.sql", "r") as f:
        # Simplified schema run - in prod we migrate properly
        pass 
        # For now we assume schema.sql was run or we create table dynamically:
    
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS lease_registry (
                unique_lease_id VARCHAR(50) PRIMARY KEY,
                title_number VARCHAR(20),
                tenure VARCHAR(50),
                lease_date DATE,
                lease_term_years INTEGER,
                lease_expiry_date DATE,
                lessee_name TEXT,
                alienation_clause BOOLEAN
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lease_title ON lease_registry(title_number)"))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lease_expiry ON lease_registry(lease_expiry_date)"))

    # 2. Locate Data
    # Placeholder S3 Key - UPDATE THIS when you get the file
    remote_key = "raw/leases/registered_leases.csv" 
    local_path = "./epc_data/registered_leases.csv"
    
    if not os.path.exists(local_path):
        # Check if we can download it (Future proofing)
        if os.getenv('AWS_ACCESS_KEY_ID'):
            print(f"⬇️  Attempting to download Leases Data ({remote_key})...")
            try:
                lake.download_file(remote_key, local_path)
            except:
                print("⚠️  File not found in S3 yet. (Waiting for Land Registry approval)")
                return
        else:
             print("⚠️  File not found locally and no AWS keys. Aborting.")
             return
    else:
        print("✅ Leases Source File found locally.")

    # 3. Stream & Process
    print("🔄 Processing Lease Data...")
    
    # TODO: Update column names based on actual CSV headers from Land Registry
    # Assuming: 'Title Number', 'Term', 'Date of Lease', 'Lessee Name', etc.
    
    try:
        chunk_iter = pd.read_csv(
            local_path, 
            chunksize=BATCH_SIZE, 
            low_memory=False,
            # encoding='ISO-8859-1', # Land Registry often uses this
        )
        
        total_ingested = 0
        
        for i, df in enumerate(chunk_iter):
            
            # CLEAN & TRANSFORM
            # 1. Filter valid leases
            # 2. Calculate Expiry
            
            # Mock transformation logic (Replace with real column mapping):
            # df['lease_expiry_date'] = pd.to_datetime(df['Date of Lease']) + pd.to_timedelta(df['Term'], unit='Y')
            
            # For now, since we don't have the file, we stop here.
            # This script serves as the architectural placeholder.
            pass
            
            # df.to_sql('lease_registry', engine, if_exists='append', index=False)
            
            # total_ingested += len(df)
            # print(f"   ✅ Batch {i}: Added {len(df)} leases...")

        print("=========================================")
        print(f"🎉 LEASE INGESTION COMPLETE.")
        print(f"📊 Total Leases Loaded: {total_ingested}")
        print("=========================================")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        print("   (Likely because the CSV structure needs to be defined once we have the file)")

if __name__ == "__main__":
    ingest_leases()
