-- the primary purpose of this table is to store order header details for use in coarse financial analytics per country.
-- a secondary purpose is to serve as operational data for the order-centric analytics based on status
CREATE TABLE IF NOT EXISTS events.fact_order_header (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    order_ts TIMESTAMP,
    status STRING,
    shipping_address_street STRING,
    shipping_address_city STRING,
    shipping_address_country STRING,
    total_amount DECIMAL,
    _inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) 
PARTITION BY order_date
CLUSTER BY 
    shipping_address_country, -- for country-based analytics
    status          -- for status-based analytics within country
;