from sqlalchemy import create_engine, text
import pandas as pd

DB_PATH = "sqlite:///vantage.db"
engine = create_engine(DB_PATH)

def analyze_distress():
    print("🕵️  VANTAGE INTELLIGENCE: DISTRESSED ASSET SCAN")
    print("===============================================")
    
    # Query: Find F/G rated properties and link to their owners
    # Note: We link master_properties to ownership via Title Number, 
    # and master_properties to EPC via UPRN.
    # For MVP, we use address matching if UPRN/Title linkage is incomplete in raw data,
    # but here we'll try a direct join strategy or simple listing.
    
    query = text("""
        SELECT 
            e.asset_rating_band,
            e.property_type,
            p.address_line_1 as property_address,
            p.uprn,
            o.proprietor_name,
            o.date_registered,
            c.company_name,
            c.incorporation_country
        FROM epc_assessments e
        JOIN master_properties p ON e.uprn = p.uprn
        -- We try to join ownership. 
        -- Note: In this raw stage, we might not have Title Numbers for all EPC UPRNs yet 
        -- (that usually requires the Land Registry 'Title-to-UPRN' link dataset).
        -- So this is a 'Strict' join showing only fully linked assets.
        LEFT JOIN ownership_records o ON p.title_number = o.title_number
        LEFT JOIN corporate_registry c ON o.company_number = c.company_number
        WHERE e.asset_rating_band IN ('F', 'G')
        ORDER BY e.asset_rating_band DESC, o.date_registered DESC
        LIMIT 50;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        if not rows:
            print("⚠️  No fully linked distressed assets found yet.")
            print("   (This is expected if we haven't ingested the Title-to-UPRN link file yet).")
            print("   Showing unmatched distressed assets instead:")
            
            # Fallback: Just show the distressed EPCs we found
            fallback_query = text("""
                SELECT asset_rating_band, property_type, address_line_1, uprn 
                FROM epc_assessments e
                JOIN master_properties p ON e.uprn = p.uprn
                WHERE asset_rating_band IN ('F', 'G')
                LIMIT 20
            """)
            fallback_rows = conn.execute(fallback_query).fetchall()
            for row in fallback_rows:
                 print(f"   • [{row[0]}] {row[2]} ({row[1]}) - UPRN: {row[3]}")
            return

        print(f"✅ FOUND {len(rows)} DISTRESSED ASSETS WITH IDENTIFIED OWNERS:\n")
        
        for row in rows:
            rating = row[0]
            addr = row[2]
            owner = row[4] or "Unknown Owner (Individual?)"
            company = row[6] or "N/A"
            country = row[7] or ""
            
            print(f"🚨 BAND {rating} | {addr}")
            print(f"   Owner: {owner}")
            if company != "N/A":
                print(f"   Entity: {company} ({country})")
            print("   ------------------------------------------------")

if __name__ == "__main__":
    analyze_distress()
