import difflib
from sqlalchemy import create_engine, text
import pandas as pd

# --- CONFIGURATION ---
DB_PATH = "sqlite:///vantage.db"

def normalize_address(addr):
    if not addr: return ""
    return addr.upper().replace(",", " ").replace(".", "").replace("  ", " ").strip()

def analyze_comps(target_postcode):
    print(f"\n📊 VALUATION ENGINE: Comps Analysis for {target_postcode}")
    print("==================================================")
    
    engine = create_engine(DB_PATH)
    
    with engine.connect() as conn:
        # 1. Fetch Recent Sales in Postcode (The "Comps")
        query_sales = text("""
            SELECT transaction_id, transfer_date, price_paid, full_address, property_type
            FROM raw_ppd_staging
            WHERE postcode = :pc
            ORDER BY transfer_date DESC
            LIMIT 20
        """)
        sales = conn.execute(query_sales, {"pc": target_postcode}).fetchall()
        
        if not sales:
            print("⚠️  No recent sales found in this postcode to generate comps.")
            return

        print(f"   Found {len(sales)} recent transactions in {target_postcode}.")
        print("   Calculating £/sqft via EPC Cross-Reference...\n")
        
        valid_comps = 0
        total_psf = 0
        
        print(f"   {'DATE':<12} | {'ADDRESS':<35} | {'PRICE':<10} | {'SIZE (SQM)':<10} | {'£/SQFT':<10}")
        print("   " + "-"*90)
        
        # 2. Enrich Sales with EPC Data (The "Alpha Hack")
        for sale in sales:
            sale_id, date, price, addr, ptype = sale
            date_str = date
            
            # Find matching EPC for this sold property to get Size
            # We look for EPCs in same postcode
            query_epc = text("""
                SELECT address_line_1, floor_area 
                FROM epc_assessments e
                JOIN master_properties p ON e.uprn = p.uprn
                WHERE p.postcode = :pc
            """)
            epc_candidates = conn.execute(query_epc, {"pc": target_postcode}).fetchall()
            
            # Fuzzy Match Address
            best_area = None
            norm_sale_addr = normalize_address(addr)
            
            for epc in epc_candidates:
                epc_addr, area = epc
                norm_epc_addr = normalize_address(epc_addr)
                
                # Check for high similarity
                ratio = difflib.SequenceMatcher(None, norm_sale_addr, norm_epc_addr).ratio()
                if ratio > 0.8: # High confidence match
                    best_area = area
                    break
            
            # Display Row
            if best_area and best_area > 0:
                # Calc Metrics
                price_per_sqm = price / best_area
                price_per_sqft = price_per_sqm / 10.764
                
                print(f"   {str(date_str):<12} | {addr[:35]:<35} | £{price/1000:.0f}k{' '*4} | {int(best_area):<10} | £{int(price_per_sqft)}")
                
                valid_comps += 1
                total_psf += price_per_sqft
            else:
                # Show sale but without size metrics
                print(f"   {str(date_str):<12} | {addr[:35]:<35} | £{price/1000:.0f}k{' '*4} | {'N/A':<10} | {'-'}")

        print("   " + "-"*90)
        
        if valid_comps > 0:
            avg_psf = total_psf / valid_comps
            print(f"\n✅ VALUATION SIGNAL: Average Sold Price = £{int(avg_psf)} / sq ft")
        else:
            print("\n⚠️  Could not calculate £/sqft (No EPC size matches found for sold units).")
            print("   Action: Ingest full EPC dataset to increase match rate.")

if __name__ == "__main__":
    # Test on one of our known distressed postcodes (from analyze_distress output)
    # E.g., 'E1 1BY' (Whitechapel Road)
    test_postcode = "E1 1BY" 
    analyze_comps(test_postcode)
