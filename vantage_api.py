from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import os
from vantage_companies import CompaniesHouseRegistry

# Initialize the App
app = FastAPI(title="Vantage Intelligence Engine")

# Allow the React App to talk to this API (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, we would lock this down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Connection
DB_PATH = "sqlite:///vantage.db"
engine = create_engine(DB_PATH)

# Initialize Companies House Registry
ch_registry = CompaniesHouseRegistry()

@app.get("/")
def read_root():
    return {"status": "Vantage System Online", "version": "0.6 (Intel Layer)"}

@app.get("/api/distress-scan")
def scan_distress():
    """
    Returns the top 50 distressed assets (Rated F or G) with Ownership Data
    """
    # Updated query to use the NEW schema
    query = text("""
        SELECT 
            e.uprn, 
            p.address_line_1 as address, 
            e.asset_rating_band, 
            e.floor_area, 
            e.property_type,
            p.local_authority_code as local_authority,
            c.company_name,
            c.company_number,
            c.company_status
        FROM epc_assessments e
        JOIN master_properties p ON e.uprn = p.uprn
        LEFT JOIN ownership_records o ON p.title_number = o.title_number
        LEFT JOIN corporate_registry c ON o.company_number = c.company_number
        WHERE e.asset_rating_band IN ('F', 'G') 
        ORDER BY e.asset_rating_band DESC, e.floor_area DESC
        LIMIT 50
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            rows = [dict(row._mapping) for row in result]
        return {"count": len(rows), "data": rows}
    except Exception as e:
        # Fallback if new schema is empty (for demo safety)
        print(f"Schema Error: {e}")
        return {"count": 0, "data": []}

@app.get("/api/search")
def search_property(q: str):
    """
    Search for a property by address
    """
    query = text("""
        SELECT p.uprn, p.address_line_1 as address, e.asset_rating_band 
        FROM master_properties p
        LEFT JOIN epc_assessments e ON p.uprn = e.uprn
        WHERE p.address_line_1 LIKE :search 
        LIMIT 10
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"search": f"%{q}%"})
        rows = [dict(row._mapping) for row in result]
        
    return rows

# --- NEW CORPORATE INTELLIGENCE ENDPOINTS ---

@app.get("/api/company/search")
def search_company(q: str):
    """
    Live search against Companies House API
    """
    if not q: return []
    result = ch_registry.search_company(q)
    return result if result else {"message": "No match found"}

@app.get("/api/company/{company_number}/structure")
def get_company_structure(company_number: str):
    """
    Builds the 'Corporate Veil' tree: Company -> Officers -> PSCs
    """
    # 1. Profile
    profile = ch_registry.get_company_profile(company_number)
    if not profile:
        raise HTTPException(status_code=404, detail="Company not found")
        
    # 2. Officers (Directors)
    officers = ch_registry.get_company_officers(company_number)
    
    # 3. PSCs (Owners)
    pscs = ch_registry.get_psc(company_number)
    
    return {
        "profile": profile,
        "officers": officers[:5], # Top 5
        "beneficial_owners": pscs[:5] # Top 5
    }

@app.get("/api/company/{company_number}/charges")
def get_company_charges(company_number: str):
    """
    Returns outstanding debts/charges to visualize 'Timeline of Squeeze'
    """
    charges = ch_registry.get_charges(company_number)
    return {"count": len(charges), "data": charges}

if __name__ == "__main__":
    import uvicorn
    print("🚀 Vantage API starting on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)