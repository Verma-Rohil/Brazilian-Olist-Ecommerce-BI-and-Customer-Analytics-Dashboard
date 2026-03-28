-- =====================================================
-- E-Commerce BI Project - EDA Queries
-- Run after data loading is complete
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- 1. BASELINE KPIs
-- =====================================================

-- Overall summary KPIs
SELECT
    COUNT(DISTINCT o.order_id)              AS total_orders,
    COUNT(DISTINCT o.customer_id)           AS total_customers,
    COUNT(DISTINCT oi.product_id)           AS total_products,
    COUNT(DISTINCT oi.seller_id)            AS total_sellers,
    ROUND(SUM(oi.total_value), 2)           AS total_revenue,
    ROUND(AVG(oi.total_value), 2)           AS avg_item_value,
    ROUND(SUM(oi.total_value) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM fact_orders o
JOIN fact_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered';


-- =====================================================
-- 2. ORDER STATUS DISTRIBUTION
-- =====================================================

SELECT
    order_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_orders), 2) AS pct
FROM fact_orders
GROUP BY order_status
ORDER BY order_count DESC;


-- =====================================================
-- 3. MONTHLY ORDER VOLUME TREND
-- =====================================================

SELECT
    order_month,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_id) AS customers
FROM fact_orders
WHERE order_status = 'delivered'
GROUP BY order_month
ORDER BY order_month;


-- =====================================================
-- 4. REVENUE BY CUSTOMER STATE (Top 10)
-- =====================================================

SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id)    AS total_orders,
    ROUND(SUM(oi.total_value), 2) AS total_revenue,
    ROUND(AVG(oi.total_value), 2) AS avg_item_value
FROM fact_orders o
JOIN fact_order_items oi ON o.order_id = oi.order_id
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 10;


-- =====================================================
-- 5. TOP 10 PRODUCT CATEGORIES BY REVENUE
-- =====================================================

SELECT
    p.product_category_english AS category,
    COUNT(DISTINCT oi.order_id) AS order_count,
    COUNT(oi.item_id)           AS items_sold,
    ROUND(SUM(oi.total_value), 2) AS total_revenue,
    ROUND(AVG(oi.price), 2)       AS avg_price
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
JOIN fact_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_english
ORDER BY total_revenue DESC
LIMIT 10;


-- =====================================================
-- 6. PAYMENT TYPE DISTRIBUTION
-- =====================================================

SELECT
    payment_type,
    COUNT(*) AS payment_count,
    ROUND(SUM(payment_value), 2) AS total_value,
    ROUND(AVG(payment_value), 2) AS avg_value,
    ROUND(AVG(payment_installments), 1) AS avg_installments,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_payments), 2) AS pct
FROM fact_payments
GROUP BY payment_type
ORDER BY payment_count DESC;


-- =====================================================
-- 7. REVIEW SCORE DISTRIBUTION
-- =====================================================

SELECT
    review_score,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_reviews), 2) AS pct
FROM fact_reviews
GROUP BY review_score
ORDER BY review_score;


-- =====================================================
-- 8. AVERAGE DELIVERY TIME BY STATE (Top 10 Slowest)
-- =====================================================

SELECT
    c.customer_state,
    COUNT(*) AS deliveries,
    ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days,
    ROUND(AVG(o.delivery_days_estimated), 1) AS avg_estimated_days,
    ROUND(AVG(o.delivery_delta_days), 1) AS avg_delta_days,
    ROUND(SUM(o.is_late_delivery) * 100.0 / COUNT(*), 2) AS late_pct
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.delivery_days_actual IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_days DESC
LIMIT 10;
