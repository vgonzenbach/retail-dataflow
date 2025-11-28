CREATE TABLE IF NOT EXISTS events.fact_order_header (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    shipping_address STRUCT<street STRING, city STRING, country STRING>,
    total_amount DECIMAL,
    _meta_inserted_at TIMESTAMP
    --_meta_source_system STRING -- can be used to identify source system
) 
PARTITION BY TIMESTAMP_TRUNC(order_date, DAY) 
CLUSTER BY 
    status, -- allows status-based analytics
    customer_id -- for joining with dim_customer + finding specific customers
;