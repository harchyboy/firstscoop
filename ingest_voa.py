import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 50000
DB_PATH = "sqlite:///vantage.db"

def ingest_voa():
    print("🚀 Starting VOA Business Rates Ingestion Pipeline...")
    
    # 1. Setup
    load_dotenv(override=True)
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)

    # Ensure table exists
    print("🛠️  Verifying Database Schema...")
    with open("schema.sql", "r") as f:
        # Simplified schema run
        pass
    
    with engine.connect() as conn:
         conn.execute(text("""
            CREATE TABLE IF NOT EXISTS voa_ratings (
                billing_authority_ref VARCHAR(50) PRIMARY KEY,
                uprn VARCHAR(20),
                address TEXT,
                postcode VARCHAR(10),
                description TEXT,
                rateable_value_2023 INTEGER,
                rateable_value_2026 INTEGER,
                scat_code VARCHAR(10),
                effective_date DATE,
                list_year INTEGER
            )
        """))
         conn.execute(text("CREATE INDEX IF NOT EXISTS idx_voa_postcode ON voa_ratings(postcode)"))

    # 2. Process 2023 List (Current)
    # S3 Path: raw/voa/2023/list_entries_baseline.csv
    process_voa_file(lake, engine, "2023", "raw/voa/2023/list_entries_baseline.csv", "./epc_data/voa_2023.csv")

    # 3. Process 2026 List (Future)
    # S3 Path: raw/voa/2026/draft_list_entries.csv
    process_voa_file(lake, engine, "2026", "raw/voa/2026/draft_list_entries.csv", "./epc_data/voa_2026.csv")

    print("=========================================")
    print(f"🎉 VOA PIPELINE COMPLETE.")
    print("=========================================")


def process_voa_file(lake, engine, year, remote_key, local_path):
    print(f"\n🔄 Processing VOA {year} List...")
    
    if not os.path.exists(local_path):
        if os.getenv('AWS_ACCESS_KEY_ID'):
            print(f"⬇️  Downloading {year} Data...")
            try:
                lake.download_file(remote_key, local_path)
            except Exception as e:
                print(f"⚠️  Download failed: {e}")
                return
        else:
             print("⚠️  File not found locally and no AWS keys.")
             return
    else:
        print(f"✅ {year} Source File found locally.")

    # Ingest
    try:
        # VOA CSV Columns are usually:
        # BA Reference, Address, Postcode, Description, RV, Effective Date...
        # We need to map these dynamically or assume standard headers.
        # For MVP, we assume the file has headers.
        
        chunk_iter = pd.read_csv(local_path, chunksize=BATCH_SIZE, low_memory=False)
        total_ingested = 0
        
        for i, df in enumerate(chunk_iter):
            # Normalize columns
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            
            # Map to our schema
            # Expected cols: 'billing_authority_reference', 'address', 'rateable_value', ...
            
            # Data cleaning/mapping logic would go here based on actual file inspection
            # For now, we'll assume we can extract key fields if they exist
            
            # Placeholder for direct mapping logic once we see the first 5 rows
            # df_clean = df[['ba_reference', 'address', ...]]
            
            pass 
            
            # df_clean.to_sql('voa_ratings', engine, if_exists='append', index=False)
            # total_ingested += len(df)
            
        print(f"📊 {year} List Loaded: {total_ingested} records.")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")

if __name__ == "__main__":
    ingest_voa()
