# Vantage Intelligence Engine 🏢🇬🇧

**The "Bloomberg Terminal" for UK Commercial Property.**

Vantage is a data intelligence platform that unifies scattered UK property datasets (EPC, Land Registry, Companies House, VOA, PlanIt, TfL, FSA, Ofcom) to identify distressed assets and their corporate owners before they hit the market.

---

## 🚀 System Architecture

The engine runs on a local **SQLite** database (for MVP) powered by a **Python** ingestion pipeline.

| Module | Function | Source Data |
|--------|----------|-------------|
| **1. Ingest** | `vantage_ingest.py` | **EPC Certificates** (S3) |
| **2. Ownership** | `ingest_ccod.py` | **Land Registry CCOD** (S3) |
| **3. Transactions** | `ingest_ppd.py` | **Land Registry PPD** (S3) |
| **4. Spatial** | `ingest_spatial.py` | **OS Code-Point & UPRN** (S3) |
| **5. VOA** | `ingest_voa.py` | **Business Rates** (Vacancy Signal) |
| **6. Planning** | `ingest_planning.py` | **PlanIt API** (Lapsed Consents) |
| **7. Mobility** | `ingest_mobility.py` | **TfL API** (Footfall Heatmaps) |
| **8. FSA** | `ingest_fsa.py` | **Hygiene Ratings** (Ghost Town Signal) |
| **9. Broadband** | `ingest_connectivity.py` | **Ofcom Data** (Tech Desperation) |
| **10. Leases** | `ingest_leases.py` | **Registered Leases** (Pending Approval) |
| **11. Covenants** | `ingest_covenants.py` | **Restrictive Covenants** (Risk Flag) |
| **12. Link** | `match_addresses.py` | **Fuzzy Logic** (Bridge datasets) |
| **13. Enrich** | `enrich_owners.py` | **Companies House API** (Directors & Debt) |
| **14. Valuation** | `analyze_comps.py` | **Sales + EPC Join** (Calc £/sqft) |
| **15. Report** | `analyze_distress.py` | **Intelligence Report** |
| **16. API** | `vantage_api.py` | **FastAPI** (Serves the UI) |

---

## 🛠️ Setup & Installation

### 1. Prerequisites
- Python 3.9+
- AWS Credentials (for S3 Data Lake access)
- Companies House API Key (optional, for enrichment)

### 2. Environment Variables
Create a `.env` file in the root directory:

```bash
# AWS S3 Access (Data Lake)
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=eu-west-2

# Companies House (Enrichment)
COMPANIES_HOUSE_KEY=your_ch_api_key

# Ordnance Survey (Maps)
OS_API_KEY=your_os_key
```

### 3. Install Dependencies
```bash
pip install pandas sqlalchemy psycopg2-binary boto3 python-dotenv requests fastapi uvicorn pyproj
```

---

## 🏃‍♂️ Running the Intelligence Pipeline

Follow this sequence to build the intelligence graph from scratch.

### Step 1: Initialize Database & Schema
```bash
# (Happens automatically when running ingest scripts, but schema defined here)
cat schema.sql
```

### Step 2: Ingest Spatial Data (The "Compass")
Loads OS Code-Point Open to enable coordinate lookups and radius searches.
```bash
python ingest_spatial.py
```

### Step 3: Ingest Ownership Data (The "Who")
Loads 4M+ corporate land ownership records from Land Registry (CCOD).
```bash
python ingest_ccod.py
```

### Step 4: Ingest Sales History (The "Comps")
Loads 5M+ recent property transactions (since 2020) to enable valuation modeling.
```bash
python ingest_ppd.py
```

### Step 5: Ingest VOA Data (The "Liability")
Loads Business Rates (2023 Current + 2026 Future) to spot tax hikes and vacancy.
```bash
python ingest_voa.py
```

### Step 6: Ingest EPC Data (The "Risk")
Loads Energy Performance Certificates to identify F/G rated assets.
```bash
python vantage_ingest.py
```

### Step 7: Ingest Planning History (The "Intent")
Scans PlanIt API for lapsed consents (permissions expiring soon).
```bash
python ingest_planning.py
```

### Step 8: Ingest Mobility Data (The "Flow")
Scans TfL API for transport nodes to build footfall/vibrancy heatmaps.
```bash
python ingest_mobility.py
```

### Step 9: Ingest Retail Signals (The "Pulse")
Scans FSA Food Hygiene ratings to find decaying high streets.
```bash
python ingest_fsa.py
```

### Step 10: Ingest Connectivity (The "Pipe")
Loads Ofcom broadband data to find fiber deserts.
```bash
python ingest_connectivity.py
```

### Step 11: Link Datasets (The "Magic")
Uses fuzzy logic to bridge the gap between EPC Addresses and Land Registry Titles.
```bash
python match_addresses.py
```

### Step 12: Generate Intelligence Report
Queries the graph to find Distressed Assets linked to Corporate Owners.
```bash
python analyze_distress.py
```

### Step 13: Deep Dive Enrichment
Fetches Director details and **Debt/Charge Maturity** dates to spot financial distress.
```bash
python enrich_owners.py
```

### Step 14: Valuation Analysis
Runs the "Comps Engine" to calculate £/sqft for a specific target area.
```bash
# Edit the postcode in the script to target a specific area
python analyze_comps.py
```

---

## 🖥️ Launching the API & UI

To serve the data to the frontend "Terminal" interface:

1. **Start the API Backend:**
   ```bash
   python vantage_api.py
   ```
   *Runs on http://localhost:8000*

2. **Start the React Frontend:**
   ```bash
   cd vantage-ui
   npm start
   ```
   *Runs on http://localhost:3000*

---

## 📊 Data Dictionary (Core Tables)

- **`master_properties`**: The central index (UPRN + Title Number).
- **`postcode_index`**: Mapping from Postcode -> Lat/Lng (OSGB36/WGS84).
- **`epc_assessments`**: Energy ratings (A-G), floor area, dates.
- **`voa_ratings`**: Rateable Value (Tax) and Use Class.
- **`planning_history`**: Application status and expiry dates.
- **`mobility_metrics`**: Footfall proxies (Station exits, Bus stops).
- **`fsa_ratings`**: Hygiene ratings and dates (Retail freshness).
- **`connectivity_metrics`**: Download speeds and 5G availability.
- **`ownership_records`**: Link table between Title and Company.
- **`transaction_history`**: Sales price, date, and type.
- **`corporate_registry`**: Company details, status, and debt flags.
- **`lease_registry`**: (Coming Soon) Lease terms and expiry dates.
- **`covenant_registry`**: (Coming Soon) Binary flag for restrictive covenants.

---

## 🔮 Future Roadmap

- [ ] **Registered Leases**: Ingest 2.2GB dataset to calculate Income Yield and Expiry Cliffs.
- [ ] **Restrictive Covenants**: Flag titles with development blockers (Monetization Trigger).
- [ ] **Planning Data Ingest**: Add planning applications to spot development potential.
- [ ] **Geospatial Visualization**: Render cadastral parcels on Mapbox (PostGIS migration).
- [ ] **UBO Graph**: Recursive graph traversal to find Ultimate Beneficial Owners (Neo4j).
- [ ] **Register of Overseas Entities**: Ingest ROE dataset for offshore ownership transparency.

---
*Vantage Intelligence © 2025*
