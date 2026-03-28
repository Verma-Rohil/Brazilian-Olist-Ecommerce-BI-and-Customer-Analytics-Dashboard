-- =====================================================
-- E-Commerce BI Project - MySQL Schema Design
-- Database: ecommerce_analytics
-- =====================================================

CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;

-- =====================================================
-- DIMENSION TABLES (load these first)
-- =====================================================

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id           VARCHAR(50) PRIMARY KEY,
    customer_unique_id    VARCHAR(50),
    customer_city         VARCHAR(100),
    customer_state        VARCHAR(5),
    customer_zip_prefix   VARCHAR(10)
);

-- Dimension: Products (with English category names merged)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id                 VARCHAR(50) PRIMARY KEY,
    product_category           VARCHAR(100),
    product_category_english   VARCHAR(100),
    product_weight_g           INT,
    product_length_cm          DECIMAL(10,2),
    product_height_cm          DECIMAL(10,2),
    product_width_cm           DECIMAL(10,2),
    product_photos_qty         INT,
    product_name_length        INT,
    product_description_length INT
);

-- Dimension: Sellers
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id         VARCHAR(50) PRIMARY KEY,
    seller_city       VARCHAR(100),
    seller_state      VARCHAR(5),
    seller_zip_prefix VARCHAR(10)
);

-- Dimension: Geolocation (aggregated to unique zip prefixes)
CREATE TABLE IF NOT EXISTS dim_geolocation (
    geolocation_zip_prefix VARCHAR(10) PRIMARY KEY,
    geolocation_lat        DECIMAL(10,6),
    geolocation_lng        DECIMAL(10,6),
    geolocation_city       VARCHAR(100),
    geolocation_state      VARCHAR(5)
);

-- =====================================================
-- FACT TABLES (load after dimensions)
-- =====================================================

-- Fact: Orders (central fact table)
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                      VARCHAR(50) PRIMARY KEY,
    customer_id                   VARCHAR(50),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      DATETIME,
    order_approved_at             DATETIME,
    order_delivered_carrier_date  DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    -- Derived columns (populated during ETL)
    delivery_days_actual          INT,         -- (delivered - purchase) in days
    delivery_days_estimated       INT,         -- (estimated - purchase) in days
    delivery_delta_days           INT,         -- estimated - actual (negative = late)
    is_late_delivery              TINYINT,     -- 1 if delivered after estimate
    order_month                   VARCHAR(7),  -- YYYY-MM for time-series & cohorts
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- Fact: Order Items (line-level detail)
CREATE TABLE IF NOT EXISTS fact_order_items (
    item_id         INT AUTO_INCREMENT PRIMARY KEY,
    order_id        VARCHAR(50),
    order_item_id   INT,
    product_id      VARCHAR(50),
    seller_id       VARCHAR(50),
    shipping_limit_date DATETIME,
    price           DECIMAL(10,2),
    freight_value   DECIMAL(10,2),
    total_value     DECIMAL(10,2),   -- derived: price + freight_value
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id)
);

-- Fact: Payments
CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id           INT AUTO_INCREMENT PRIMARY KEY,
    order_id             VARCHAR(50),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

-- Fact: Reviews
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id                VARCHAR(50) PRIMARY KEY,
    order_id                 VARCHAR(50),
    review_score             INT,
    review_comment_title     TEXT,
    review_comment_message   TEXT,
    review_creation_date     DATETIME,
    review_answer_timestamp  DATETIME,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

-- =====================================================
-- INDEXES (for query performance)
-- =====================================================

CREATE INDEX idx_orders_customer        ON fact_orders(customer_id);
CREATE INDEX idx_orders_status          ON fact_orders(order_status);
CREATE INDEX idx_orders_purchase_date   ON fact_orders(order_purchase_timestamp);
CREATE INDEX idx_orders_month           ON fact_orders(order_month);
CREATE INDEX idx_orders_late            ON fact_orders(is_late_delivery);
CREATE INDEX idx_items_order            ON fact_order_items(order_id);
CREATE INDEX idx_items_product          ON fact_order_items(product_id);
CREATE INDEX idx_items_seller           ON fact_order_items(seller_id);
CREATE INDEX idx_payments_order         ON fact_payments(order_id);
CREATE INDEX idx_payments_type          ON fact_payments(payment_type);
CREATE INDEX idx_reviews_order          ON fact_reviews(order_id);
CREATE INDEX idx_reviews_score          ON fact_reviews(review_score);
CREATE INDEX idx_customers_unique       ON dim_customers(customer_unique_id);
CREATE INDEX idx_products_category      ON dim_products(product_category_english);
CREATE INDEX idx_sellers_state          ON dim_sellers(seller_state);
