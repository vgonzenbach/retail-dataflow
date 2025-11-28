CREATE TABLE IF NOT EXISTS events.fact_order_items (
    order_item_id STRING,
    order_id STRING,
    order_date TIMESTAMP,
    product_id STRING, 
    product_name STRING,
    quantity INTEGER, 
    price DECIMAL,
    _meta_inserted_at TIMESTAMP
    --_meta_source_system STRING -- can be used to identify source system
) 
PARTITION BY TIMESTAMP_TRUNC(order_date, DAY) 
CLUSTER BY 
    product_id, -- allows product-based analytics
    order_id -- for joining with fact_order_header + finding specific orders