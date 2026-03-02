-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    month INT,
    quarter INT,
    day_of_week VARCHAR(20)
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_sustainability (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    product_id INT REFERENCES dim_product(product_id),
    units_sold INT,
    revenue NUMERIC(12,2),
    co2_kg NUMERIC(10,4),
    energy_kwh NUMERIC(10,4),
    waste_kg NUMERIC(10,4),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Pipeline Log Table
CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    run_time TIMESTAMP DEFAULT NOW(),
    source VARCHAR(100),
    records_processed INT,
    records_failed INT,
    status VARCHAR(20),
    message TEXT
);
