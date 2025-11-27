import os
import pandas as pd
from sqlalchemy import create_engine, text
from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 10000  # Process 10k rows at a time
DB_PATH = "sqlite:///vantage.db"

def ingest_ccod():
    print("🚀 Starting CCOD Ownership Ingestion Pipeline...")
    
    # 1. Setup Connections
    load_dotenv()
    lake = VantageDataLake()
    engine = create_engine(DB_PATH)
    
    # Ensure database schema exists
    print("🛠️  Verifying Database Schema...")
    with open("schema.sql", "r") as f:
        schema_sql = f.read()
        # Split by semicolon to execute valid statements one by one
        statements = schema_sql.split(';')
        with engine.connect() as conn:
            for statement in statements:
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()

    # 2. Locate Data
    remote_key = "raw/ccod/2025-11/CCOD_FULL_2025_11.csv"
    local_path = "./epc_data/CCOD_FULL_2025_11.csv"
    
    # Download if missing
    if not os.path.exists(local_path):
        print(f"⬇️  Downloading CCOD Source File ({remote_key})...")
        lake.download_file(remote_key, local_path)
    else:
        print("✅ CCOD Source File found locally.")

    # 3. Stream & Process (Chunking for large files)
    print("🔄 Processing Data Streams...")
    
    chunk_iter = pd.read_csv(
        local_path, 
        chunksize=BATCH_SIZE, 
        low_memory=False,
        encoding='utf-8', 
        dtype=str # Read all as string initially to avoid type errors
    )
    
    total_records = 0
    
    for i, df in enumerate(chunk_iter):
        
        # CLEANUP: Rename columns to match our internal standard if needed
        # Expected CCOD columns: [Title Number, Tenure, Property Address, District, County, Region, Postcode, Multiple Address Indicator, Price Paid, Proprietor Name (1), Company Registration No. (1), Proprietorship Category (1), Country Incorporated (1), ...]
        
        # We focus on the First Proprietor for MVP simplicity
        
        # Prepare Corporate Registry Data (The Companies)
        # We extract unique companies from this chunk
        # NOTE: 'Country Incorporated' is missing in this dataset version, so we default to Unknown or derive later
        companies = df[['Company Registration No. (1)', 'Proprietor Name (1)', 'Proprietorship Category (1)']].copy()
        companies['incorporation_country'] = 'Unknown' # Placeholder
        
        companies.columns = ['company_number', 'company_name', 'company_category', 'incorporation_country']
        companies = companies.dropna(subset=['company_number'])
        companies = companies.drop_duplicates(subset=['company_number'])
        
        # Prepare Ownership Records (The Links)
        # We also need to construct a full address for the proprietor
        df['proprietor_address_full'] = df['Proprietor (1) Address (1)'].fillna('') + ' ' + df['Proprietor (1) Address (2)'].fillna('') + ' ' + df['Proprietor (1) Address (3)'].fillna('')
        
        ownership = df[['Title Number', 'Company Registration No. (1)', 'Proprietor Name (1)', 'proprietor_address_full', 'Price Paid', 'Date Proprietor Added']].copy()
        ownership.columns = ['title_number', 'company_number', 'proprietor_name', 'proprietor_address', 'price_paid', 'date_registered']
        ownership = ownership.dropna(subset=['title_number', 'company_number'])
        
        # Prepare Master Properties Stubs (To satisfy foreign keys)
        # In a full run, we'd have these from OS MasterMap, but we create stubs here
        properties = df[['Title Number', 'Property Address', 'Postcode']].copy()
        properties.columns = ['title_number', 'address_line_1', 'postcode']
        properties = properties.drop_duplicates(subset=['title_number'])
        
        # WRITE TO DB
        with engine.connect() as conn:
            # 1. Properties (Insert OR IGNORE)
            # SQLite doesn't have robust "ON CONFLICT" in pandas to_sql, so we use raw SQL for safer merges or just append carefully
            # For MVP, we will use 'append' but in prod we need upsert logic.
            # We'll stick to a simple strategy: Try to insert, ignore errors if duplicates exist (handled by DB constraints usually, but pandas to_sql can fail).
            
            # Actually, standard pandas to_sql fails on duplicates. 
            # Strategy: Write to temp table, then INSERT OR IGNORE into main table.
            
            # A. Companies
            companies.to_sql('temp_companies', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT OR IGNORE INTO corporate_registry (company_number, company_name, incorporation_country, company_category)
                SELECT company_number, company_name, incorporation_country, company_category FROM temp_companies
            """))
            
            # B. Properties (Stub records)
            properties.to_sql('temp_properties', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT OR IGNORE INTO master_properties (title_number, address_line_1, postcode)
                SELECT title_number, address_line_1, postcode FROM temp_properties
            """))

            # C. Ownership
            # We assume ownership records are unique per title/company combo in our simplified view
            ownership.to_sql('temp_ownership', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT OR REPLACE INTO ownership_records (title_number, company_number, proprietor_name, proprietor_address, price_paid, date_registered)
                SELECT title_number, company_number, proprietor_name, proprietor_address, price_paid, date_registered FROM temp_ownership
            """))
            
            conn.commit()

        count = len(ownership)
        total_records += count
        print(f"   ✅ Batch {i+1}: Ingested {count} ownership links...")

    print("=========================================")
    print(f"🎉 INGESTION COMPLETE.")
    print(f"📊 Total Ownership Records: {total_records}")
    print("=========================================")

if __name__ == "__main__":
    ingest_ccod()
