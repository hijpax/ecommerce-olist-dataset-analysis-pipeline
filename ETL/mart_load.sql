-- Carga de datos en dim_seller
MERGE INTO dim_seller AS target
USING (
  SELECT DISTINCT seller_id
  FROM order_items
) AS source
ON target.seller_id = source.seller_id
WHEN NOT MATCHED THEN
  INSERT (seller_id)
  VALUES (source.seller_id);

-- Llenar tabla dim_city
MERGE INTO dim_city AS target
USING (
  SELECT DISTINCT customer_city, customer_state
  FROM customers
) AS source
ON target.city = source.customer_city AND target.state = source.customer_state
WHEN NOT MATCHED THEN
  INSERT (city, state)
  VALUES (source.customer_city, source.customer_state);

-- Llenar tabla dim_zip_code
MERGE INTO dim_zip_code AS target
USING (
  SELECT DISTINCT customer_zip_code_prefix
  FROM customers
) AS source
ON target.zip_code_prefix = source.customer_zip_code_prefix
WHEN NOT MATCHED THEN
  INSERT (zip_code_prefix)
  VALUES (source.customer_zip_code_prefix);

-- Llenar tabla dim_customer
MERGE INTO dim_customer AS target
USING (
  SELECT
    customer_id,
    customer_unique_id,
    zip_code_id,
    city_id
  FROM customers
    JOIN dim_zip_code ON customers.customer_zip_code_prefix = dim_zip_code.zip_code_prefix
    JOIN dim_city ON customers.customer_city = dim_city.city AND customers.customer_state = dim_city.state
) AS source
ON target.customer_id = source.customer_id
WHEN NOT MATCHED THEN
  INSERT (customer_id,customer_unique_id, zip_code_id, city_id)
  VALUES (source.customer_id,source.customer_unique_id, source.zip_code_id, source.city_id);

-- Llenar tabla dim_order_status
MERGE INTO dim_order_status AS target
USING (
  SELECT DISTINCT order_status
  FROM orders
) AS source
ON target.order_status = source.order_status
WHEN NOT MATCHED THEN
  INSERT (order_status)
  VALUES (source.order_status);

-- Llenar tabla dim_product
MERGE INTO dim_product AS target
USING (
  SELECT DISTINCT product_id
  FROM order_items
) AS source
ON target.product_id = source.product_id
WHEN NOT MATCHED THEN
  INSERT (product_id)
  VALUES (source.product_id);


-- Carga de datos en fact_order_items
MERGE INTO fact_order_items AS target
USING (
  SELECT
    oi.order_item_id,
    oi.price,
    oi.freight_value,
    o.customer_id,
    oi.product_id,
    dc.city_id AS city_id,
    os.order_status_id AS order_status_id,
    s.seller_id,
    purchase_ddate.date AS purchase_date,
    purchase_dtime.time AS purchase_time,
    approved_ddate.date AS approved_date,
    approved_dtime.time AS approved_time,
    delivered_carrier_ddate.date AS delivered_carrier_date,
    delivered_carrier_dtime.time AS delivered_carrier_time,
    delivered_ddate.date AS delivered_customer_date,
    delivered_dtime.time AS delivered_customer_time,
    estimated_delivery_ddate.date AS estimated_delivery_date,
    estimated_delivery_dtime.time AS estimated_delivery_time,
    shipping_limit_ddate.date AS shipping_limit_date,
    shipping_limit_dtime.time AS shipping_limit_time
  FROM order_items AS oi
    JOIN orders AS o ON oi.order_id = o.order_id
    JOIN dim_customer AS dc ON o.customer_id = dc.customer_id
    JOIN dim_order_status AS os ON o.order_status = os.order_status
    JOIN dim_product AS p ON oi.product_id = p.product_id
    JOIN dim_seller AS s ON oi.seller_id = s.seller_id
    JOIN dim_date AS purchase_ddate ON DATE(o.order_purchase_timestamp) = ddate.date
    JOIN dim_time AS purchase_dtime ON TIME(o.order_purchase_timestamp) = dtime.time
    JOIN dim_date AS approved_ddate ON DATE(o.order_approved_at) = ddate.date
    JOIN dim_time AS approved_dtime ON TIME(o.order_approved_at) = dtime.time
    JOIN dim_date AS delivered_carrier_ddate ON DATE(o.order_delivered_carrier_date) = ddate.date
    JOIN dim_time AS delivered_carrier_dtime ON TIME(o.order_delivered_carrier_date) = dtime.time
    JOIN dim_date AS delivered_ddate ON DATE(o.order_delivered_customer_date) = ddate.date
    JOIN dim_time AS delivered_dtime ON TIME(o.order_delivered_customer_date) = dtime.time
    JOIN dim_date AS estimated_delivery_ddate ON DATE(o.order_estimated_delivery_date) = ddate.date
    JOIN dim_time AS estimated_delivery_dtime ON TIME(o.order_estimated_delivery_date) = dtime.time
    JOIN dim_date AS shipping_limit_ddate ON DATE(oi.shipping_limit_date) = ddate.date
    JOIN dim_time AS shipping_limit_dtime ON TIME(oi.shipping_limit_date) = dtime.time
) AS source
ON target.order_item_id = source.order_item_id
WHEN NOT MATCHED THEN
  INSERT (
    order_item_id,
    price,
    freight_value,
    customer_id,
    product_id,
    city_id,
    order_status_id,
    seller_id,
    purchase_date_id,
    purchase_time_id,
    approved_date_id,
    approved_time_id,
    delivered_carrier_date_id,
    delivered_carrier_time_id,
    delivered_customer_date_id,
    delivered_customer_time_id,
    estimated_delivery_date_id,
    estimated_delivery_time_id,
    shipping_limit_date_id,
    shipping_limit_time_id
  )
  VALUES (
    source.order_item_id,
    source.price,
    source.freight_value,
    source.customer_id,
    source.product_id,
    source.city_id,
    source.order_status_id,
    source.seller_id,
    (SELECT id FROM dim_date WHERE date = source.purchase_date),
    (SELECT id FROM dim_time WHERE time = source.purchase_time),
    (SELECT id FROM dim_date WHERE date = source.approved_date),
    (SELECT id FROM dim_time WHERE time = source.approved_time),
    (SELECT id FROM dim_date WHERE date = source.delivered_carrier_date),
    (SELECT id FROM dim_time WHERE time = source.delivered_carrier_time),
    (SELECT id FROM dim_date WHERE date = source.delivered_customer_date),
    (SELECT id FROM dim_time WHERE time = source.delivered_customer_time),
    (SELECT id FROM dim_date WHERE date = source.estimated_delivery_date),
    (SELECT id FROM dim_time WHERE time = source.estimated_delivery_time),
    (SELECT id FROM dim_date WHERE date = source.shipping_limit_date),
    (SELECT id FROM dim_time WHERE time = source.shipping_limit_time)
  );
