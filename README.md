# Streaming Dataflow Pipeline for e-commerce BQ data ingestion

This a streaming Dataflow pipeline for ingesting e-commerce data into BigQuery. 

## Data Models

![images/erd.png](./images/erd.png)

The present data model is a Kimball/dimensional model with the following tables:
    - fact_order_header
    - fact_order_item
    - fact_order
    - fact_inventory
    - fact_user_activity

DDL statements can be found in the [sql](./sql) folder.

### Order Header

`fact_order_header` is a table that contains the order details at the header level. It is used for coarse financial analytics per country. A secondary purpose is to serve as operational data for the order-centric analytics based on status. Hence, the table is partitioned by order_date and clustered by shipping_address_country and status. This allows country-level and status-level aggregations to be performed efficiently. 

For instance, the following query counts the number of delivered orders per country:

```sql
SELECT 
    shipping_address_country,
    COUNT(*) AS delivered_orders
FROM 
    events.fact_order_header
WHERE 
    order_date = '2025-12-01'
    AND status = 'delivered'
GROUP BY 
    shipping_address_country
ORDER BY 
    shipping_address_country

```

`fact_order_header` is derived from order events by dropping item-level information and flattening the address fields to allow clustering on coushipping_address_country and for improving usability of the end-user. 

### Order Item

`fact_order_item` is a table that contains the order details at the item level. It is used for product-centric analytics. The table is partitioned by order_date and clustered by product_id. This allows product-level aggregations to be performed efficiently. It can be joined with the fact_inventory table efficientlyto get the current inventory quantity for a product.

For instance, the following query counts the number of orders per product:

```sql
SELECT 
    product_id,
    MAX(product_name) AS product_name, -- pick any
    COUNT(DISTINCT order_id) AS orders,
    SUM(quantity) AS total_items_sold,
    SUM(total_amount) AS total_revenue
FROM 
    events.fact_order_item
GROUP BY 
    product_id
ORDER BY 
    product_id
```

`fact_order_item` is derived from order events by exploding the items array, computing total_amount for each line item and enriching with order-level details such as order_id (for reference) and order_date (for partitioning).


### Fact Order

`fact_order` is a table that contains all information of an order event a item-level granularity. Nested Fields are flattened/exploded to allow for maximum usability. 

The purpose of this table is to be used for customer-centric analytics. It is partitioned by order_date and clustered by customer_id. This allows customer-level aggregations to be performed efficiently. It can be joined with the fact_user_activity table efficiently as well. 

A secondary clustering on order_id allows for compression of the table since the explosion of the items array accounts for "duplicate" order-level data. By clustering on order_id, some storage savings can be achieved thanks to RLE compression.

As an example of usage, the following query counts the number of orders per customer:

```sql
SELECT 
    customer_id,
    MAX(customer_name) AS customer_name, -- pick any
    COUNT(DISTINCT order_id) AS orders,
    SUM(total_amount) AS total_revenue    
FROM 
    events.fact_order
GROUP BY 
    customer_id
ORDER BY 
    customer_id
```

Note that an argment could be made that this table is redundant with the previous two. In terms of storage it certainly is, but the different clustering can be used to optimize queries for different purposes. The user is encouraged to use the previous two tables if their use case requires only header-related or item-related data not both as for the objectives mentioned above. Conversely, `fact_order` is provided to avoid joins for cases where both header and item-level data are required.


### Inventory

An inventory is a table that contains the inventory movements for a product. It can be used for product-centric analytics regarding stock movements thanks to the clustering on product and reason.

For instance, the following query shows the products with the most returns:

```sql
SELECT 
    product_id,
    SUM(quantity_change) AS n_returns
FROM 
    events.fact_inventory
GROUP BY 
    product_id, reason
HAVING reason = 'return'
ORDER BY 2 DESC 
```

### User Activity

`fact_user_activity` is a table that contains the user activity details for user-centric analytics. It is partitioned by event_date and clustered by user_id and sesson_id. This allows user-level and session-level aggregations to be performed efficiently including session-based windowing.

As a basic example, we can assign a step-in-session number to each activity in a user-session:

```sql  
SELECT
  user_id,
  session_id,
  event_timestamp,
  activity_type,
  ROW_NUMBER() OVER (
    PARTITION BY user_id, session_id
    ORDER BY event_timestamp
  ) AS step_in_session
FROM events.fact_user_activity;
```


#### Further Notes

All tables contains both date and timestamp columns for usability. An ingestion timestamp is automatically added to each row by BQ.


## Dataflow Pipeline

The Dataflow pipeline is defined in the [pipeline.py](./pipeline.py) file. It is a streaming pipeline that ingests data from Pub/Sub and writes to the above-mentioned BigQuery tables. 

Functionalities include:

    - Type-safe transformation
    - Data validation
    - Dead-letter queueing for invalid data in GCS
    - Validation of schema contracts (not implemented yet)
    - Dead-letter queueing for unknown events (not implemented yet)
    - Modeling of fact based on events
    - Ingestion of data into BigQuery

The custom transformations are defined in the [transforms](./transform) directory. Details regarding the implementation are left to the reader.

### Usage

To run the pipeline, execute the [run.sh](./run.sh) script and a Dataflow job will be created. Prior steps need to be taken to set up the needed infrastructure such as:
    - Create a Pub/Sub topic
    - Create a Pub/Sub subscription
    - Create a BigQuery dataset
    - Create the BigQuery tables by running the DDL statements in the [sql](./sql) folder
    - Create a GCS bucket
    - Enable the Dataflow API

Once the infrastructure is set up, 








ToDo:

1. save to BQ:
  - split items table

2. extend pipeline:

  - flatten address
  - create order table items
    composite key order_id|item_id (sort by price to get determinisitic order)

3. validate schema and data

4. error handling

5. dataflow runner

6. monitoring


# Future work



Questions:

Window based on event timestamp or stream time? 
    - naming based on event time 
    - but windowing based on stream time?

Error handling:
    - what if pubsub fails?
    - what if gcs fails?
    - what if bigquery fails?
    - what if data is malformed?
    - what if data is missing?
    - what if data is invalid?
    - what if event type is invalid?
        - what if event type is not in the list of event types?

Pending:
    - save ingestion time in BQ


documentation: 
    fact_order_header table:
        clustering on status, customer_id:
            allows queries like:
                how many order delivered today?
                how many orders pending?
            customer_id as 2nd cluster allows (less efficiently):
                joins with dim_customer
                customer-centric analytics
            
    fact_order_items table:
        clustering on product_id, order_id:
            allows queries like:
                how many orders have a specific product?
            order_id as 2nd cluster allows (less efficiently):
                joins with fact_order_header

    why not more clustering?
        - clustering adjustment overhead with streaming 
