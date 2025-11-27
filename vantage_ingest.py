import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
DB_CONN = "sqlite:///vantage.db"

def ingest_pipeline():
    print("\n🚀 Starting Vantage Cloud Pipeline (EPC Module)...")
    
    # Setup
    load_dotenv()
    lake = VantageDataLake()
    
    if not os.path.exists("./epc_data"):
        os.makedirs("./epc_data")

    # 1. SCAN S3 FOR CERTIFICATES
    print(f"📡 Scanning S3 Bucket '{lake.bucket_name}' for certificate files...")
    target_files = []
    
    try:
        response = lake.s3_client.list_objects_v2(Bucket=lake.bucket_name, Prefix="raw/epc/")
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if "certificates.csv" in key and "recommendations" not in key:
                    target_files.append(key)
    except Exception as e:
        print(f"❌ Error Listing S3 Files: {e}")
        return

    print(f"✅ Found {len(target_files)} EPC files to process.")

    # 2. CONNECT TO DB
    engine = create_engine(DB_CONN)
    
    # 3. LOOP THROUGH FILES
    total_ingested = 0
    
    for s3_key in target_files:
        safe_name = s3_key.replace("/", "_")
        local_path = os.path.join("./epc_data", safe_name)
        
        print(f"\n⬇️  Processing: {s3_key}")
        
        if not os.path.exists(local_path):
            lake.download_file(s3_key, local_path)
        else:
            print(f"   ✅ File already exists locally: {local_path}")

        try:
            # Read CSV
            df = pd.read_csv(local_path, low_memory=False)
            df.columns = [c.lower() for c in df.columns]
            
            # --- MAPPING TO NEW SCHEMA ---
            
            # 1. UPSERT PROPERTIES (We need the UPRN in master_properties first)
            # We use the address from EPC as a fallback if not already in DB
            properties = df[['uprn', 'address', 'postcode', 'local_authority']].copy()
            properties.columns = ['uprn', 'address_line_1', 'postcode', 'local_authority_code']
            properties = properties.dropna(subset=['uprn'])
            properties = properties.drop_duplicates(subset=['uprn'])
            
            # 2. PREPARE ASSESSMENTS
            assessments = df[[
                'lmk_key', 'uprn', 'inspection_date', 'asset_rating', 
                'asset_rating_band', 'floor_area', 'property_type'
            ]].copy()
            assessments.columns = [
                'certificate_id', 'uprn', 'inspection_date', 'asset_rating', 
                'asset_rating_band', 'floor_area', 'property_type'
            ]
            assessments['is_latest'] = 1 # Simplified assumption for now
            assessments = assessments.dropna(subset=['certificate_id'])

            # WRITE TO DB
            with engine.connect() as conn:
                # A. Properties (Insert or Ignore)
                properties.to_sql('temp_epc_props', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    INSERT OR IGNORE INTO master_properties (uprn, address_line_1, postcode, local_authority_code)
                    SELECT uprn, address_line_1, postcode, local_authority_code FROM temp_epc_props
                """))
                
                # B. Assessments (Insert or Replace)
                assessments.to_sql('temp_epc_assessments', conn, if_exists='replace', index=False)
                conn.execute(text("""
                    INSERT OR REPLACE INTO epc_assessments 
                    (certificate_id, uprn, inspection_date, asset_rating, asset_rating_band, floor_area, property_type, is_latest)
                    SELECT certificate_id, uprn, inspection_date, asset_rating, asset_rating_band, floor_area, property_type, is_latest 
                    FROM temp_epc_assessments
                """))
                conn.commit()

            count = len(assessments)
            total_ingested += count
            print(f"   ✅ Linked {count} EPCs to Master Property Index.")
            
        except Exception as e:
            print(f"   ❌ Error Ingesting: {e}")

    print("=========================================")
    print(f"🎉 BATCH COMPLETE.")
    print(f"📊 Total EPCs Ingested: {total_ingested}")
    print("=========================================")

if __name__ == "__main__":
    ingest_pipeline()