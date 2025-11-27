import difflib
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"
CONFIDENCE_THRESHOLD = 0.85  # 85% similarity required

def normalize_address(addr):
    """
    Simple normalization to improve matching odds.
    """
    if not addr: return ""
    return addr.upper().replace(",", " ").replace(".", "").replace("  ", " ").strip()

def match_addresses():
    print("🔗 STARTING TACTICAL ADDRESS MATCHING (Fuzzy Logic)")
    print("==================================================")
    
    engine = create_engine(DB_PATH)
    
    # 1. Get all Distressed EPCs (Targets)
    # We only care about ones that don't have a Title Number yet
    print("📡 Fetching unlinked distressed assets...")
    
    with engine.connect() as conn:
        # Get EPC properties (Band F/G) that have a Postcode but NO Title Number linkage yet
        query_targets = text("""
            SELECT p.uprn, p.address_line_1, p.postcode, e.asset_rating_band
            FROM master_properties p
            JOIN epc_assessments e ON p.uprn = e.uprn
            WHERE e.asset_rating_band IN ('F', 'G')
              AND (p.title_number IS NULL OR p.title_number = '')
              AND p.postcode IS NOT NULL
        """)
        targets = conn.execute(query_targets).fetchall()
        
        print(f"   found {len(targets)} distressed assets needing ownership data.")
        
        matches_found = 0
        
        # 2. Loop through each target
        for target in targets:
            uprn, addr_epc, postcode, band = target
            norm_addr_epc = normalize_address(addr_epc)
            
            # 3. Find candidates in CCOD with SAME Postcode
            # We look for rows in master_properties that HAVE a Title Number (from CCOD ingest)
            # and match the postcode.
            query_candidates = text("""
                SELECT title_number, address_line_1 
                FROM master_properties 
                WHERE postcode = :pc 
                  AND title_number IS NOT NULL
            """)
            candidates = conn.execute(query_candidates, {"pc": postcode}).fetchall()
            
            best_match = None
            highest_ratio = 0.0
            
            for cand in candidates:
                title, addr_ccod = cand
                norm_addr_ccod = normalize_address(addr_ccod)
                
                # Check similarity
                ratio = difflib.SequenceMatcher(None, norm_addr_epc, norm_addr_ccod).ratio()
                
                if ratio > highest_ratio:
                    highest_ratio = ratio
                    best_match = cand
            
            # 4. Evaluate Match
            if highest_ratio >= CONFIDENCE_THRESHOLD:
                title_match = best_match[0]
                addr_match = best_match[1]
                
                print(f"✅ MATCH FOUND ({int(highest_ratio*100)}%):")
                print(f"   EPC:  {addr_epc} (UPRN: {uprn})")
                print(f"   Land: {addr_match} (Title: {title_match})")
                
                # 5. EXECUTE LINK
                # We update the EPC row in master_properties to include the Title Number.
                # This effectively bridges the graph!
                update_link = text("""
                    UPDATE master_properties 
                    SET title_number = :title 
                    WHERE uprn = :uprn
                """)
                conn.execute(update_link, {"title": title_match, "uprn": uprn})
                conn.commit()
                
                matches_found += 1
            
    print("==================================================")
    print(f"🎉 LINKING COMPLETE.")
    print(f"🔗 Successfully connected {matches_found} properties to their owners.")
    print("==================================================")

if __name__ == "__main__":
    match_addresses()
