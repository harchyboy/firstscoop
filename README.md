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
| **3. Link** | `match_addresses.py` | **Fuzzy Logic** (Bridge datasets) |
| **4. Enrich** | `enrich_owners.py` | **Companies House API** |
| **5. Analyze** | `analyze_distress.py` | **SQL Intelligence Queries** |
| **6. API** | `vantage_api.py` | **FastAPI** (Serves the UI) |

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

### Step 3: Ingest EPC Data (The "Risk")
Loads Energy Performance Certificates to identify F/G rated assets.
```bash
python vantage_ingest.py
```

### Step 4: Link Datasets (The "Magic")
Uses fuzzy logic to bridge the gap between EPC Addresses and Land Registry Titles.
```bash
python match_addresses.py
```

### Step 5: Generate Intelligence Report
Queries the graph to find Distressed Assets linked to Corporate Owners.
```bash
python analyze_distress.py
```

### Step 6: Deep Dive Enrichment (Optional)
Fetches Director/Financial details for the identified owners.
```bash
python enrich_owners.py
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
- **`corporate_registry`**: Company details (Name, Country, Status).

---

## 🔮 Future Roadmap

- [ ] **Planning Data Ingest**: Add planning applications to spot development potential.
- [ ] **Price Paid Data**: Link historical transaction values.
- [ ] **Geospatial Visualization**: Render cadastral parcels on Mapbox.
- [ ] **Director Graph**: Map relationships between directors across companies.

---
*Vantage Intelligence © 2025*

