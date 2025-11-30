-- the purpose of this table is to store order details at the granularity of items
-- for use in customer-centric analytics in conjuction with the fact_user_activity table
CREATE TABLE IF NOT EXISTS events.fact_order (
    -- header fields
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    order_ts TIMESTAMP,
    status STRING,
    shipping_address_street STRING,
    shipping_address_city STRING,
    shipping_address_country STRING,
    total_amount DECIMAL,

    -- items fields
    product_id STRING, 
    product_name STRING,
    quantity INTEGER, 
    price DECIMAL,
    item_amount DECIMAL,

    -- technical fields
    _inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY order_date
CLUSTER BY
    customer_id,               -- efficient join with customer_id and user_activity
    order_id                  -- RLE storage savings on header level fields + efficient windowing by order_id
;
