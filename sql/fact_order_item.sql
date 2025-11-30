-- the purpose of this table is to store item-level details for use in product-centric analytics
-- in conjunction with a hypothetical dim_product table or the given fact_inventory table
CREATE TABLE IF NOT EXISTS events.fact_order_item (
    order_id STRING,
    order_date DATE,
    order_ts TIMESTAMP,
    product_id STRING, 
    product_name STRING,
    quantity INTEGER, 
    price DECIMAL,
    total_amount DECIMAL,
    _inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) 
PARTITION BY order_date
CLUSTER BY 
    product_id -- efficient join with dim_product + inventory
;
