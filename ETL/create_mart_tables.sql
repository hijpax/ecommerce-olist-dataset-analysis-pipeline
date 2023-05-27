-- Crear tabla dim_seller
CREATE TABLE dim_seller (
  seller_id STRING
);

-- Crear tabla dim_city
CREATE TABLE dim_city (
  city_id INT64,
  city STRING,
  state STRING
);

-- Crear tabla dim_zip_code
CREATE TABLE dim_zip_code (
  zip_code_id INT64,
  zip_code_prefix INT64
);

-- Crear tabla dim_customer
CREATE TABLE dim_customer (
  zip_code_id INT64,
  city_id INT64,
  customer_unique_id STRING
);

-- Crear tabla dim_order_status
CREATE TABLE dim_order_status (
  order_status_id INT64,
  order_status STRING
);

-- Crear tabla dim_product
CREATE TABLE dim_product (
  product_id STRING
);

-- Crear tabla dim_date
CREATE TABLE dim_date (
  date TIMESTAMP,
  day INT64,
  month INT64,
  year INT64
);

-- Crear tabla dim_time
CREATE TABLE dim_time (
  time TIME,
  hour12 STRING,
  hour24 INT64,
  minutes INT64,
  seconds INT64,
  am_or_pm STRING
);

-- Crear tabla fact_order_items
CREATE TABLE fact_order_items (
  order_item_unique_id INT64,
  customer_id STRING,
  product_id STRING,
  city_id INT64,
  order_status_id INT64,
  seller_id STRING,
  purchase_date_id INT64,
  purchase_time_id INT64,
  approved_date_id INT64,
  approved_time_id INT64,
  delivered_carrier_date_id INT64,
  delivered_carrier_time_id INT64,
  delivered_customer_date_id INT64,
  delivered_customer_time_id INT64,
  estimated_delivery_date_id INT64,
  estimated_delivery_time_id INT64,
  shipping_limit_date_id INT64,
  shipping_limit_time_id INT64,
  order_item_id STRING,
  price FLOAT64,
  freight_value FLOAT64
);