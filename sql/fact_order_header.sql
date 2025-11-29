CREATE TABLE IF NOT EXISTS events.fact_order_header (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    shipping_address_street STRING,
    shipping_address_city STRING,
    shipping_address_country STRING,
    total_amount DECIMAL,
    _inserted_at TIMESTAMP
    --_source_system STRING -- can be used to identify source system
) 
PARTITION BY TIMESTAMP_TRUNC(order_date, DAY) 
CLUSTER BY 
    customer_id, -- allows status-based analytics
    status -- for joining with dim_customer + finding specific customers
;