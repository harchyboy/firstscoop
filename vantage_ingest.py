import os
import pandas as pd
from sqlalchemy import create_engine
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- SETUP ---
print("🔍 SYSTEM CHECK STARTING...")
dotenv_path = os.path.join(os.getcwd(), '.env')
alt_env_path = os.path.join(os.getcwd(), 'vantage-engine.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
elif os.path.exists(alt_env_path):
    load_dotenv(alt_env_path)

# --- CTO PIVOT: SWITCH TO SQLITE ---
# We are using a local file database to bypass the server requirement.
# This creates a file named 'vantage.db' in your current folder.
print("⚠️ SWITCHING TO SQLITE (File-based Database)")
DB_CONN = "sqlite:///vantage.db"

def ingest_pipeline():
    print("\n🚀 Starting Vantage Cloud Pipeline (SQLite Mode)...")
    
    lake = VantageDataLake()
    
    # Create local storage folder
    if not os.path.exists("./epc_data"):
        os.makedirs("./epc_data")

    # 1. SCAN S3 FOR ALL CERTIFICATES
    print(f"📡 Scanning S3 Bucket '{lake.bucket_name}' for all certificate files...")
    
    target_files = []
    
    try:
        # List everything in the raw/epc folder
        response = lake.s3_client.list_objects_v2(Bucket=lake.bucket_name, Prefix="raw/epc/")
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # FILTER: Look for 'certificates.csv' but ignore 'recommendations'
                if "certificates.csv" in key and "recommendations" not in key:
                    target_files.append(key)
        else:
            print("❌ The 'raw/epc/' folder appears to be empty.")
            return

    except Exception as e:
        print(f"❌ Error Listing S3 Files: {e}")
        return

    print(f"✅ Found {len(target_files)} CSV files to process.")

    # 2. CONNECT TO DB (Creates the file if missing)
    try:
        engine = create_engine(DB_CONN)
        print("✅ Database Connection Established (vantage.db)")
    except Exception as e:
        print(f"❌ Database Connection Error: {e}")
        return

    # 3. LOOP THROUGH FILES
    total_ingested = 0
    
    for s3_key in target_files:
        safe_name = s3_key.replace("/", "_")
        local_path = os.path.join("./epc_data", safe_name)
        
        print(f"\n⬇️  Processing: {s3_key}")
        
        # Download
        success = lake.download_file(s3_key, local_path)
        if not success:
            print(f"   ⚠️ Skipping {s3_key} (Download Failed)")
            continue

        # Ingest
        try:
            df = pd.read_csv(local_path, low_memory=False)
            df.columns = [c.lower() for c in df.columns]
            
            valid_cols = [
                'uprn', 'address', 'asset_rating_band', 'floor_area', 'property_type',
                'lmk_key', 'building_reference_number', 'inspection_date', 'local_authority', 'asset_rating'
            ]
            
            existing_cols = [c for c in valid_cols if c in df.columns]
            df_clean = df[existing_cols]
            
            if df_clean.empty:
                print("   ⚠️ Skipped (No valid columns found)")
                continue
                
            # Push to SQL
            df_clean.to_sql('raw_epc_commercial', engine, if_exists='append', index=False)
            count = len(df_clean)
            total_ingested += count
            print(f"   ✅ Added {count} rows to Database.")
            
        except Exception as e:
            print(f"   ❌ Error Ingesting: {e}")

    print("=========================================")
    print(f"🎉 BATCH COMPLETE.")
    print(f"📊 Total Buildings Ingested: {total_ingested}")
    print("=========================================")

if __name__ == "__main__":
    ingest_pipeline()