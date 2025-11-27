from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import os

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

# Database Connection (The same file we just created)
DB_PATH = "sqlite:///vantage.db"
engine = create_engine(DB_PATH)

@app.get("/")
def read_root():
    return {"status": "Vantage System Online", "version": "0.5"}

@app.get("/api/distress-scan")
def scan_distress():
    """
    Returns the top 50 distressed assets (Rated F or G)
    """
    query = text("""
        SELECT 
            uprn, 
            address, 
            asset_rating_band, 
            floor_area, 
            property_type,
            local_authority
        FROM raw_epc_commercial 
        WHERE asset_rating_band IN ('F', 'G') 
        LIMIT 50
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        # Convert to list of dicts
        rows = [dict(row._mapping) for row in result]
        
    return {"count": len(rows), "data": rows}

@app.get("/api/search")
def search_property(q: str):
    """
    Search for a property by address
    """
    # Secure parameter binding to prevent SQL Injection
    query = text("""
        SELECT * FROM raw_epc_commercial 
        WHERE address LIKE :search 
        LIMIT 10
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"search": f"%{q}%"})
        rows = [dict(row._mapping) for row in result]
        
    return rows

if __name__ == "__main__":
    import uvicorn
    print("🚀 Vantage API starting on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)