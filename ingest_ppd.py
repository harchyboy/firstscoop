import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 50000
DB_PATH = "sqlite:///vantage.db"
START_YEAR = 2020  # Filter for recent transactions for speed

def ingest_ppd():
    print("🚀 Starting Price Paid Data (PPD) Ingestion Pipeline...")
    
    # 1. Setup
    load_dotenv()
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)
    
    # 2. Locate Data
    # S3 Key: raw/ppd/baseline/pp-complete.csv
    remote_key = "raw/ppd/baseline/pp-complete.csv"
    local_path = "./epc_data/pp-complete.csv"
    
    if not os.path.exists(local_path):
        print(f"⬇️  Downloading PPD Source File (5GB+)... This may take a while.")
        lake.download_file(remote_key, local_path)
    else:
        print("✅ PPD Source File found locally.")

    # 3. Stream & Process
    print(f"🔄 Processing Transactions from {START_YEAR} onwards...")
    
    # Columns in pp-complete.csv (No headers in raw file usually)
    # 0: Transaction ID
    # 1: Price
    # 2: Date
    # 3: Postcode
    # 4: Property Type (D, S, T, F, O) -> O = Other (Commercial potential)
    # 5: Old/New
    # 6: Duration (F/L)
    # 7: PAON (Primary Address)
    # 8: SAON (Secondary Address)
    # 9: Street
    # 10: Locality
    # 11: Town/City
    # 12: District
    # 13: County
    # 14: PPD Category Type (A = Standard, B = Additional)
    # 15: Record Status
    
    chunk_iter = pd.read_csv(
        local_path, 
        chunksize=BATCH_SIZE, 
        header=None, 
        names=[
            'id', 'price', 'date', 'postcode', 'type', 'old_new', 'duration',
            'paon', 'saon', 'street', 'locality', 'city', 'district', 'county', 'ppd_cat', 'status'
        ],
        parse_dates=['date']
    )
    
    total_ingested = 0
    
    for i, df in enumerate(chunk_iter):
        
        # FILTER: Date Range
        df_recent = df[df['date'].dt.year >= START_YEAR].copy()
        
        if df_recent.empty:
            continue
            
        # Transform for DB
        # We map to our 'transaction_history' table schema
        # transaction_id, title_number (unknown in PPD), transfer_date, price_paid, property_type, new_build_flag
        
        # Note: PPD doesn't have Title Numbers. Linking is done via Address/Postcode matching later.
        
        records = df_recent[['id', 'date', 'price', 'type', 'old_new']].copy()
        records.columns = ['transaction_id', 'transfer_date', 'price_paid', 'property_type', 'new_build_flag']
        
        # We also need to store address components to enable matching
        # For this MVP schema, transaction_history relies on title_number which we don't have here.
        # We should create a raw landing table for PPD first to allow matching logic.
        
        with engine.connect() as conn:
            # Create staging table if not exists (since schema.sql defined the 'linked' version)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_ppd_staging (
                    transaction_id VARCHAR(40) PRIMARY KEY,
                    price_paid INTEGER,
                    transfer_date DATE,
                    postcode VARCHAR(10),
                    property_type CHAR(1),
                    full_address TEXT,
                    paon TEXT, saon TEXT, street TEXT
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ppd_postcode ON raw_ppd_staging(postcode)"))
            
            # Construct rudimentary address for display/matching
            df_recent['full_address'] = (
                df_recent['paon'].fillna('') + ' ' + 
                df_recent['saon'].fillna('') + ' ' + 
                df_recent['street'].fillna('')
            ).str.strip().str.upper()
            
            upload_df = df_recent[['id', 'price', 'date', 'postcode', 'type', 'full_address', 'paon', 'saon', 'street']]
            upload_df.columns = ['transaction_id', 'price_paid', 'transfer_date', 'postcode', 'property_type', 'full_address', 'paon', 'saon', 'street']
            
            upload_df.to_sql('temp_ppd', conn, if_exists='replace', index=False)
            
            conn.execute(text("""
                INSERT OR IGNORE INTO raw_ppd_staging 
                (transaction_id, price_paid, transfer_date, postcode, property_type, full_address, paon, saon, street)
                SELECT transaction_id, price_paid, transfer_date, postcode, property_type, full_address, paon, saon, street 
                FROM temp_ppd
            """))
            conn.commit()
            
        count = len(df_recent)
        total_ingested += count
        
        if i % 10 == 0:
            print(f"   ✅ Batch {i}: Added {count} recent sales (Total: {total_ingested})...")

    print("=========================================")
    print(f"🎉 PPD INGESTION COMPLETE.")
    print(f"📊 Total Recent Sales Loaded: {total_ingested}")
    print("=========================================")

if __name__ == "__main__":
    ingest_ppd()
