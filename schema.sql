-- Create schema
CREATE SCHEMA IF NOT EXISTS airbnb;

-- Listings table (main table)
CREATE TABLE airbnb.listings (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    host_id BIGINT,
    host_name VARCHAR(255),
    neighbourhood VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    h3_cell_res9 VARCHAR(15),  -- H3 cell identifier at resolution 9
    room_type VARCHAR(50),
    price DECIMAL(11, 2),
    minimum_nights INTEGER,
    number_of_reviews INTEGER,
    last_review DATE,
    reviews_per_month DECIMAL(4, 2),
    calculated_host_listings_count INTEGER,
    availability_365 INTEGER,
    number_of_reviews_ltm INTEGER,
    license VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reviews table
CREATE TABLE airbnb.reviews (
    id SERIAL PRIMARY KEY,
    listing_id BIGINT REFERENCES airbnb.listings(id),
    date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Calendar table
CREATE TABLE airbnb.calendar (
    id SERIAL PRIMARY KEY,
    listing_id BIGINT REFERENCES airbnb.listings(id),
    date DATE,
    available BOOLEAN,
    price DECIMAL(11, 2),
    adjusted_price DECIMAL(11, 2),
    minimum_nights INTEGER,
    maximum_nights INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT calendar_listing_date_unique UNIQUE (listing_id, date)
);

-- Create indexes for better query performance
CREATE INDEX idx_listings_host_id ON airbnb.listings(host_id);
CREATE INDEX idx_listings_neighbourhood ON airbnb.listings(neighbourhood);
CREATE INDEX idx_listings_room_type ON airbnb.listings(room_type);
CREATE INDEX idx_reviews_listing_id ON airbnb.reviews(listing_id);
CREATE INDEX idx_reviews_date ON airbnb.reviews(date);
CREATE INDEX idx_calendar_listing_id ON airbnb.calendar(listing_id);
CREATE INDEX idx_calendar_date ON airbnb.calendar(date);

-- Add helpful comments to tables
COMMENT ON TABLE airbnb.listings IS 'Main table containing Airbnb listing information';
COMMENT ON TABLE airbnb.reviews IS 'Table containing review dates for listings';
COMMENT ON TABLE airbnb.calendar IS 'Table containing availability and pricing information for listings';

-- Add helpful comments to important columns
COMMENT ON COLUMN airbnb.listings.id IS 'Unique identifier for the listing';
COMMENT ON COLUMN airbnb.listings.price IS 'Price in local currency (integer)';
COMMENT ON COLUMN airbnb.listings.room_type IS 'Type of room (e.g., Entire home/apt, Private room)';
COMMENT ON COLUMN airbnb.calendar.available IS 'Whether the listing is available on the given date';
COMMENT ON COLUMN airbnb.calendar.price IS 'Price for the specific date';
COMMENT ON COLUMN airbnb.calendar.adjusted_price IS 'Adjusted price for the specific date';

-- Create a view for active listings (those with reviews in the last year)
CREATE OR REPLACE VIEW airbnb.active_listings AS
SELECT 
    l.*,
    COUNT(r.id) as review_count_last_year
FROM 
    airbnb.listings l
    LEFT JOIN airbnb.reviews r ON l.id = r.listing_id 
    AND r.date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY 
    l.id;

-- Create a view for listing availability summary
CREATE OR REPLACE VIEW airbnb.listing_availability_summary AS
SELECT 
    l.id as listing_id,
    l.name,
    l.room_type,
    COUNT(CASE WHEN c.available THEN 1 END) as available_days,
    COUNT(CASE WHEN NOT c.available THEN 1 END) as booked_days,
    AVG(c.price) as average_price,
    MIN(c.price) as minimum_price,
    MAX(c.price) as maximum_price
FROM 
    airbnb.listings l
    LEFT JOIN airbnb.calendar c ON l.id = c.listing_id
GROUP BY 
    l.id, l.name, l.room_type; 