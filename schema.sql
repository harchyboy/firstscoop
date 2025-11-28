-- =========================================================================================
-- VANTAGE INTELLIGENCE ENGINE - UNIFIED SCHEMA (SQLite Compatible)
-- =========================================================================================

-- 1. MASTER PROPERTY RECORD (The Anchor)
-- Links UPRN (Ordnance Survey) with Title Number (Land Registry)
CREATE TABLE IF NOT EXISTS master_properties (
    uprn VARCHAR(20) PRIMARY KEY,
    title_number VARCHAR(20),
    address_line_1 TEXT,
    postcode VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    local_authority_code VARCHAR(10),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_properties_title ON master_properties(title_number);
CREATE INDEX IF NOT EXISTS idx_properties_postcode ON master_properties(postcode);

-- 2. CORPORATE REGISTRY (The "Who")
-- Stores details on the companies that own land (UK & Overseas)
CREATE TABLE IF NOT EXISTS corporate_registry (
    company_number VARCHAR(20) PRIMARY KEY,
    company_name TEXT,
    incorporation_country TEXT,
    company_category TEXT, -- 'LTD', 'PLC', 'OVERSEAS'
    company_status TEXT,   -- 'ACTIVE', 'DISSOLVED', 'LIQUIDATION'
    last_accounts_date DATE,
    sic_codes TEXT
);

-- 3. LAND OWNERSHIP (The Link)
-- Derived from CCOD/OCOD. Links a Property Title to a Corporate Owner.
CREATE TABLE IF NOT EXISTS ownership_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title_number VARCHAR(20),
    company_number VARCHAR(20),
    proprietor_name TEXT,     -- Name as it appears on the deed
    proprietor_address TEXT,  -- Address of the owner
    date_registered DATE,
    price_paid DECIMAL(15, 2), -- If available in CCOD
    FOREIGN KEY(title_number) REFERENCES master_properties(title_number),
    FOREIGN KEY(company_number) REFERENCES corporate_registry(company_number)
);

CREATE INDEX IF NOT EXISTS idx_ownership_company ON ownership_records(company_number);

-- 4. ENERGY PERFORMANCE (The Risk)
-- Compliance data from EPC certificates
CREATE TABLE IF NOT EXISTS epc_assessments (
    certificate_id VARCHAR(24) PRIMARY KEY,
    uprn VARCHAR(20),
    inspection_date DATE,
    asset_rating INTEGER,      -- The raw score (0-150+)
    asset_rating_band CHAR(2), -- A-G
    floor_area NUMERIC,
    property_type TEXT,
    is_latest BOOLEAN DEFAULT 1,
    FOREIGN KEY(uprn) REFERENCES master_properties(uprn)
);

CREATE INDEX IF NOT EXISTS idx_epc_rating ON epc_assessments(asset_rating_band);
CREATE INDEX IF NOT EXISTS idx_epc_uprn ON epc_assessments(uprn);

-- 5. TRANSACTION HISTORY (The Value)
-- Historical sales data from Price Paid Data (PPD)
CREATE TABLE IF NOT EXISTS transaction_history (
    transaction_id VARCHAR(40) PRIMARY KEY,
    title_number VARCHAR(20),
    transfer_date DATE,
    price_paid DECIMAL(15, 2),
    property_type CHAR(1), -- 'D' = Detached, 'S' = Semi, etc.
    new_build_flag CHAR(1),
    FOREIGN KEY(title_number) REFERENCES master_properties(title_number)
);

CREATE INDEX IF NOT EXISTS idx_trans_title ON transaction_history(title_number);

-- 6. LEASE REGISTRY (The Income/Expiry)
-- Derived from Registered Leases dataset (2.2GB)
CREATE TABLE IF NOT EXISTS lease_registry (
    unique_lease_id VARCHAR(50) PRIMARY KEY, -- Usually Title + Lease Count
    title_number VARCHAR(20),      -- Link to Freehold Title
    tenure VARCHAR(50),            -- 'Leasehold'
    lease_date DATE,               -- Start of lease
    lease_term_years INTEGER,      -- Duration
    lease_expiry_date DATE,        -- Calculated (Start + Term)
    lessee_name TEXT,              -- The Tenant (if available)
    alienation_clause BOOLEAN,     -- Can they sublet? (If extracted)
    FOREIGN KEY(title_number) REFERENCES master_properties(title_number)
);

CREATE INDEX IF NOT EXISTS idx_lease_title ON lease_registry(title_number);
CREATE INDEX IF NOT EXISTS idx_lease_expiry ON lease_registry(lease_expiry_date);

-- 7. RESTRICTIVE COVENANTS (The "Tripwire")
-- Derived from Restrictive Covenants dataset (3.65GB)
CREATE TABLE IF NOT EXISTS covenant_registry (
    title_number VARCHAR(20) PRIMARY KEY,
    has_covenant BOOLEAN DEFAULT 1,
    last_updated DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY(title_number) REFERENCES master_properties(title_number)
);

-- 8. SPATIAL INDEX (The "Compass")
-- Derived from OS Code-Point Open. Allows fast Postcode -> Lat/Lng lookups.
CREATE TABLE IF NOT EXISTS postcode_index (
    postcode VARCHAR(10) PRIMARY KEY,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    eastings INTEGER,
    northings INTEGER,
    district_code VARCHAR(10)
);

-- 9. VOA BUSINESS RATES (The "Vacancy & Tax Signal")
-- Derived from VOA Rating Lists (2023 & 2026 Draft)
CREATE TABLE IF NOT EXISTS voa_ratings (
    billing_authority_ref VARCHAR(50) PRIMARY KEY, -- The VOA ID
    uprn VARCHAR(20),                              -- Linked via Address Match
    address TEXT,
    postcode VARCHAR(10),
    description TEXT,                              -- e.g. "Offices and Premises"
    rateable_value_2023 INTEGER,                   -- Current Tax Base
    rateable_value_2026 INTEGER,                   -- Future Tax Base (Draft)
    scat_code VARCHAR(10),                         -- Special Category (Use Class)
    effective_date DATE,
    list_year INTEGER,                             -- 2023 or 2026
    FOREIGN KEY(uprn) REFERENCES master_properties(uprn)
);

CREATE INDEX IF NOT EXISTS idx_voa_postcode ON voa_ratings(postcode);
CREATE INDEX IF NOT EXISTS idx_voa_rv ON voa_ratings(rateable_value_2023);

-- 10. PLANNING HISTORY (The "Developer Intent") -- **NEW MODULE**
-- Derived from PlanIt API. Tracks Refusals and Approvals.
CREATE TABLE IF NOT EXISTS planning_history (
    application_id VARCHAR(50) PRIMARY KEY,
    uprn VARCHAR(20),                          -- Linked to Property
    address TEXT,
    description TEXT,                          -- e.g. "Erection of 5 storey block..."
    status VARCHAR(50),                        -- 'Granted', 'Refused', 'Withdrawn'
    decision_date DATE,
    expiry_date DATE,                          -- Calculated: Granted + 3 Years
    is_lapsing_soon BOOLEAN DEFAULT 0,         -- Flag for "Snipe" opportunity
    url TEXT,                                  -- Link to full docs
    FOREIGN KEY(uprn) REFERENCES master_properties(uprn)
);

CREATE INDEX IF NOT EXISTS idx_planning_status ON planning_history(status);
CREATE INDEX IF NOT EXISTS idx_planning_expiry ON planning_history(expiry_date);

-- =========================================================================================
-- ANALYTICAL VIEWS (The "Intelligence")
-- =========================================================================================

-- View: Distressed Assets (F/G Rated) owned by Overseas Entities
CREATE VIEW IF NOT EXISTS view_distressed_overseas AS
SELECT 
    p.uprn,
    p.address_line_1,
    p.title_number,
    e.asset_rating_band,
    o.proprietor_name,
    c.incorporation_country
FROM master_properties p
JOIN epc_assessments e ON p.uprn = e.uprn
JOIN ownership_records o ON p.title_number = o.title_number
JOIN corporate_registry c ON o.company_number = c.company_number
WHERE e.asset_rating_band IN ('F', 'G')
  AND c.incorporation_country != 'United Kingdom';
