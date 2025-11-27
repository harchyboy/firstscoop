# Vantage Intelligence Engine 🏢🇬🇧

**The "Bloomberg Terminal" for UK Commercial Property.**

Vantage is a data intelligence platform that unifies scattered UK property datasets (EPC, Land Registry, Companies House) to identify distressed assets and their corporate owners before they hit the market.

---

## 🚀 System Architecture

The engine runs on a local **SQLite** database (for MVP) powered by a **Python** ingestion pipeline.

| Module | Function | Source Data |
|--------|----------|-------------|
| **1. Ingest** | `vantage_ingest.py` | **EPC Certificates** (S3) |
| **2. Ownership** | `ingest_ccod.py` | **Land Registry CCOD** (S3) |
| **3. Transactions** | `ingest_ppd.py` | **Land Registry PPD** (S3) |
| **4. Link** | `match_addresses.py` | **Fuzzy Logic** (Bridge datasets) |
| **5. Enrich** | `enrich_owners.py` | **Companies House API** (Directors & Debt) |
| **6. Valuation** | `analyze_comps.py` | **Sales + EPC Join** (Calc £/sqft) |
| **7. Report** | `analyze_distress.py` | **Intelligence Report** |
| **8. API** | `vantage_api.py` | **FastAPI** (Serves the UI) |

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
```

### 3. Install Dependencies
```bash
pip install pandas sqlalchemy psycopg2-binary boto3 python-dotenv requests fastapi uvicorn
```

---

## 🏃‍♂️ Running the Intelligence Pipeline

Follow this sequence to build the intelligence graph from scratch.

### Step 1: Initialize Database & Schema
```bash
# (Happens automatically when running ingest scripts, but schema defined here)
cat schema.sql
```

### Step 2: Ingest Ownership Data (The "Who")
Loads 4M+ corporate land ownership records from Land Registry (CCOD).
```bash
python ingest_ccod.py
```

### Step 3: Ingest Sales History (The "Comps")
Loads 5M+ recent property transactions (since 2020) to enable valuation modeling.
```bash
python ingest_ppd.py
```

### Step 4: Ingest EPC Data (The "Risk")
Loads Energy Performance Certificates to identify F/G rated assets.
```bash
python vantage_ingest.py
```

### Step 5: Link Datasets (The "Magic")
Uses fuzzy logic to bridge the gap between EPC Addresses and Land Registry Titles.
```bash
python match_addresses.py
```

### Step 6: Generate Intelligence Report
Queries the graph to find Distressed Assets linked to Corporate Owners.
```bash
python analyze_distress.py
```

### Step 7: Deep Dive Enrichment
Fetches Director details and **Debt/Charge Maturity** dates to spot financial distress.
```bash
python enrich_owners.py
```

### Step 8: Valuation Analysis
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
- **`epc_assessments`**: Energy ratings (A-G), floor area, dates.
- **`ownership_records`**: Link table between Title and Company.
- **`transaction_history`**: Sales price, date, and type.
- **`corporate_registry`**: Company details, status, and debt flags.

---

## 🔮 Future Roadmap

- [ ] **Planning Data Ingest**: Add planning applications to spot development potential.
- [ ] **Geospatial Visualization**: Render cadastral parcels on Mapbox (PostGIS migration).
- [ ] **UBO Graph**: Recursive graph traversal to find Ultimate Beneficial Owners (Neo4j).
- [ ] **Register of Overseas Entities**: Ingest ROE dataset for offshore ownership transparency.

---
*Vantage Intelligence © 2025*
