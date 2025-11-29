CREATE TABLE IF NOT EXISTS events.fact_order_items (
    order_id STRING,
    order_date TIMESTAMP,
    product_id STRING, 
    product_name STRING,
    quantity INTEGER, 
    price DECIMAL
) 
PARTITION BY TIMESTAMP_TRUNC(order_date, DAY) 
CLUSTER BY 
    order_id, -- efficient join with order header
    product_id -- efficient join with inventory + dim_product
;∏
