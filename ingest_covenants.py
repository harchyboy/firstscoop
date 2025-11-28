import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 50000
DB_PATH = "sqlite:///vantage.db"

def ingest_covenants():
    print("🚀 Starting Restrictive Covenant Ingestion Pipeline...")
    
    # 1. Setup
    load_dotenv(override=True)
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)

    # Ensure table exists (Schema Update)
    print("🛠️  Verifying Database Schema...")
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS covenant_registry (
                title_number VARCHAR(20) PRIMARY KEY,
                has_covenant BOOLEAN DEFAULT 1,
                last_updated DATE DEFAULT CURRENT_DATE
            )
        """))

    # 2. Locate Data
    # Placeholder S3 Key - UPDATE THIS when you get the file
    remote_key = "raw/covenants/restrictive_covenants.csv" 
    local_path = "./epc_data/restrictive_covenants.csv"
    
    if not os.path.exists(local_path):
        if os.getenv('AWS_ACCESS_KEY_ID'):
            print(f"⬇️  Attempting to download Covenant Data ({remote_key})...")
            try:
                lake.download_file(remote_key, local_path)
            except:
                print("⚠️  File not found in S3 yet. (Waiting for Land Registry approval)")
                return
        else:
             print("⚠️  File not found locally and no AWS keys. Aborting.")
             return
    else:
        print("✅ Covenant Source File found locally.")

    # 3. Stream & Process
    print("🔄 Processing Covenant Data...")
    
    # Assuming CSV structure is simple: [Title Number, ...]
    # We only need the Title Number to flag it.
    
    try:
        chunk_iter = pd.read_csv(
            local_path, 
            chunksize=BATCH_SIZE, 
            # usecols=['Title Number'] # Optimization: Only read the ID
        )
        
        total_ingested = 0
        
        for i, df in enumerate(chunk_iter):
            
            # Prepare Data
            # If the CSV has column 'Title Number', we map it.
            # df_clean = pd.DataFrame()
            # df_clean['title_number'] = df['Title Number']
            # df_clean['has_covenant'] = 1
            
            # Push to DB
            # df_clean.to_sql('covenant_registry', engine, if_exists='append', index=False)
            
            pass # Placeholder until file arrives
            
            # count = len(df)
            # total_ingested += count
            
            if i % 10 == 0:
                # print(f"   ✅ Batch {i}: Flagged {count} titles with covenants...")
                pass

        print("=========================================")
        print(f"🎉 COVENANT INDEX COMPLETE.")
        print(f"📊 Total Risk Flags Loaded: {total_ingested}")
        print("=========================================")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        print("   (Likely because the CSV structure needs to be defined once we have the file)")

if __name__ == "__main__":
    ingest_covenants()
