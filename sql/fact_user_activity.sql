-- the purpose of this table is to store user activity details for user-centric analytics with efficient windowing by session_id
CREATE TABLE IF NOT EXISTS events.fact_user_activity (
    user_id STRING,
    session_id STRING,
    platform STRING,
    activity_type STRING,
    ip_address STRING,
    user_agent STRING,
    event_date DATE,
    event_timestamp TIMESTAMP,
    _inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY event_date
CLUSTER BY 
    user_id, -- efficient joins with dim_customer and fact_order
    session_id -- efficient windowing by session_id
;