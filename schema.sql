CREATE TABLE IF NOT EXISTS raw_epc_commercial (
    id SERIAL PRIMARY KEY,
    uprn VARCHAR(20),
    address TEXT,
    asset_rating_band CHAR(2), -- 'E', 'F', 'G'
    floor_area NUMERIC,
    property_type TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast searching
CREATE INDEX idx_epc_rating ON raw_epc_commercial(asset_rating_band);