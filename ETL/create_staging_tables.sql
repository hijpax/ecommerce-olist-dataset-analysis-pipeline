-- Creación de la tabla "customers"
CREATE TABLE customers (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix INT64,
  customer_city STRING,
  customer_state STRING
);

-- Creación de la tabla "orders"
CREATE TABLE orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

-- Creación de la tabla "order_items"
CREATE TABLE order_items (
  order_id STRING,
  order_item_id STRING,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date TIMESTAMP,
  price FLOAT64,
  freight_value FLOAT64
);
