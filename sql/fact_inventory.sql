-- the purpose of this table is to store inventory movements for product-centric analytics
CREATE TABLE IF NOT EXISTS events.fact_inventory (
    inventory_id STRING,
    product_id STRING,
    warehouse_id STRING,
    quantity_change INTEGER,
    reason STRING,
    event_date DATE,
    event_timestamp TIMESTAMP,
    _inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY event_date
CLUSTER BY
    product_id, -- efficient join with order_items within a warehouse
    reason
;

