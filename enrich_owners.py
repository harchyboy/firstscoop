from sqlalchemy import create_engine, text
from vantage_companies import CompaniesHouseRegistry
import time

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"

def enrich_owners():
    print("🕵️  STARTING CORPORATE INTELLIGENCE ENRICHMENT")
    print("============================================")
    
    engine = create_engine(DB_PATH)
    ch = CompaniesHouseRegistry()
    
    if not ch.api_key:
        print("❌ STOP: No Companies House API Key found.")
        print("   Please add COMPANIES_HOUSE_KEY=your_key to .env")
        return

    # 1. Identify Target Companies
    # Companies linked to F/G properties that haven't been enriched yet (or have 'Unknown' country)
    with engine.connect() as conn:
        query = text("""
            SELECT DISTINCT c.company_number, c.company_name
            FROM corporate_registry c
            JOIN ownership_records o ON c.company_number = o.company_number
            JOIN master_properties p ON o.title_number = p.title_number
            JOIN epc_assessments e ON p.uprn = e.uprn
            WHERE e.asset_rating_band IN ('F', 'G')
              AND (c.company_status IS NULL OR c.incorporation_country = 'Unknown')
            LIMIT 10
        """)
        targets = conn.execute(query).fetchall()
    
    print(f"🎯 Targeted {len(targets)} companies for deep dive analysis.\n")
    
    for row in targets:
        local_id = row[0]     # The ID we have locally (might be from CCOD)
        name = row[1]         # Company Name
        
        print(f"🔍 Investigating: {name} (Ref: {local_id})...")
        
        # A. SEARCH (Resolve to official Company Number)
        # CCOD IDs are sometimes internal LR refs, so we search by name to be sure
        match = ch.search_company(name)
        
        if match:
            official_number = match.get('company_number')
            title = match.get('title')
            status = match.get('company_status', 'active')
            addr = match.get('address_snippet', '')
            
            print(f"   ✅ MATCHED: {title} (#{official_number})")
            print(f"      Status: {status.upper()}")
            print(f"      HQ: {addr}")
            
            # B. GET OFFICERS (Directors)
            officers = ch.get_company_officers(official_number)
            director_names = [o.get('name', 'Unknown') for o in officers if 'resigned_on' not in o][:3] # Top 3 active
            
            print(f"      Directors: {', '.join(director_names)}")
            
            # C. UPDATE DATABASE
            # We update our local registry with this gold-dust info
            with engine.connect() as conn:
                update_sql = text("""
                    UPDATE corporate_registry 
                    SET company_number = :new_id, -- Correct the ID if needed
                        company_status = :status,
                        incorporation_country = 'United Kingdom',
                        sic_codes = :directors -- Abusing SIC column for now to store directors for MVP
                    WHERE company_number = :old_id
                """)
                # Note: Changing PK (company_number) is risky if cascade isn't set. 
                # For MVP, we'll just update attributes and keep old ID if it matches, 
                # or we might have to handle the ID swap carefully.
                # Let's just update the status for now to be safe.
                
                update_safe = text("""
                    UPDATE corporate_registry 
                    SET company_status = :status,
                        incorporation_country = 'United Kingdom',
                        company_name = :official_name
                    WHERE company_number = :old_id
                """)
                
                conn.execute(update_safe, {
                    "status": status,
                    "official_name": title,
                    "old_id": local_id
                })
                conn.commit()
                
            print("   💾 Database Updated.")
            
        else:
            print("   ⚠️  No match found in Companies House (Foreign Entity?)")
        
        # Respect API Rate Limits
        time.sleep(0.6) 

    print("\n============================================")
    print("🎉 ENRICHMENT COMPLETE")

if __name__ == "__main__":
    enrich_owners()
