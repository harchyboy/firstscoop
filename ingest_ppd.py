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
    # Explicitly reload environment variables to ensure AWS keys are present
    load_dotenv(override=True)
    
    # Check if keys are actually loaded
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        print("⚠️  Warning: AWS Credentials not found. Will only process local files.")
        # Continue...
    else:
        lake = VantageDataLake()
    
    engine = create_engine(DB_PATH)
    
    # Ensure staging table exists first
    with engine.connect() as conn:
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

    # --- PART A: BASELINE (The History) ---
    remote_key_baseline = "raw/ppd/baseline/pp-complete.csv"
    local_path_baseline = "./epc_data/pp-complete.csv"
    
    if not os.path.exists(local_path_baseline):
        print(f"⬇️  Downloading PPD Baseline (5GB+)...")
        lake.download_file(remote_key_baseline, local_path_baseline)
    else:
        print("✅ PPD Baseline found locally.")

    process_ppd_file(local_path_baseline, engine, "Baseline", filter_year=START_YEAR)

    # --- PART B: MONTHLY UPDATE (The Freshness) ---
    remote_key_monthly = "raw/ppd/monthly/pp-monthly-update-new-version.csv"
    local_path_monthly = "./epc_data/pp-monthly-update.csv"
    
    # Check credentials before attempting download
    if os.getenv('AWS_ACCESS_KEY_ID'):
        print(f"⬇️  Downloading PPD Monthly Update...")
        try:
            lake.download_file(remote_key_monthly, local_path_monthly)
            process_ppd_file(local_path_monthly, engine, "Monthly Update", filter_year=None)
        except Exception as e:
            print(f"⚠️  Could not download monthly update (Check AWS Keys): {e}")
    else:
        print("⚠️  Skipping Monthly Update (AWS Keys missing). Using Baseline data only.")

    print("=========================================")
    print(f"🎉 PPD PIPELINE COMPLETE.")
    print("=========================================")


def process_ppd_file(filepath, engine, label, filter_year=None):
    print(f"\n🔄 Processing {label} ({filepath})...")
    
    chunk_iter = pd.read_csv(
        filepath, 
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
        
        # FILTER: Date Range (Only for baseline, monthly is always relevant)
        if filter_year:
            df_recent = df[df['date'].dt.year >= filter_year].copy()
        else:
            df_recent = df.copy()
        
        if df_recent.empty:
            continue
            
        with engine.connect() as conn:
            # Construct rudimentary address for display/matching
            df_recent['full_address'] = (
                df_recent['paon'].fillna('') + ' ' + 
                df_recent['saon'].fillna('') + ' ' + 
                df_recent['street'].fillna('')
            ).str.strip().str.upper()
            
            upload_df = df_recent[['id', 'price', 'date', 'postcode', 'type', 'full_address', 'paon', 'saon', 'street']]
            upload_df.columns = ['transaction_id', 'price_paid', 'transfer_date', 'postcode', 'property_type', 'full_address', 'paon', 'saon', 'street']
            
            upload_df.to_sql('temp_ppd', conn, if_exists='replace', index=False)
            
            # Upsert (Replace if transaction ID exists, usually implies an update/correction)
            conn.execute(text("""
                INSERT OR REPLACE INTO raw_ppd_staging 
                (transaction_id, price_paid, transfer_date, postcode, property_type, full_address, paon, saon, street)
                SELECT transaction_id, price_paid, transfer_date, postcode, property_type, full_address, paon, saon, street 
                FROM temp_ppd
            """))
            conn.commit()
            
        count = len(df_recent)
        total_ingested += count
        
        if i % 10 == 0:
            print(f"   ✅ Batch {i}: Added {count} sales...")

    print(f"   📊 {label} Loaded: {total_ingested} records.")

if __name__ == "__main__":
    ingest_ppd()