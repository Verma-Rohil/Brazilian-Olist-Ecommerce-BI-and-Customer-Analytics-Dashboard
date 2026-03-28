-- =====================================================
-- E-Commerce BI Project - Delivery & Logistics Analytics
-- Advanced SQL: SLA tracking, delivery-review correlation
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- 1. DELIVERY SLA PERFORMANCE BY STATE
-- Skills: CTE, conditional aggregation, percentage calcs
-- =====================================================

WITH delivery_metrics AS (
    SELECT
        c.customer_state,
        o.order_id,
        o.delivery_days_actual,
        o.delivery_days_estimated,
        o.delivery_delta_days,
        o.is_late_delivery
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
)
SELECT
    customer_state,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(delivery_days_actual), 1) AS avg_delivery_days,
    ROUND(AVG(delivery_days_estimated), 1) AS avg_estimated_days,
    ROUND(AVG(delivery_delta_days), 1) AS avg_delta_days,
    MIN(delivery_days_actual) AS min_days,
    MAX(delivery_days_actual) AS max_days,
    SUM(is_late_delivery) AS late_deliveries,
    ROUND(SUM(is_late_delivery) * 100.0 / COUNT(*), 2) AS late_delivery_pct,
    ROUND((1 - SUM(is_late_delivery) * 1.0 / COUNT(*)) * 100, 2) AS on_time_pct
FROM delivery_metrics
GROUP BY customer_state
ORDER BY late_delivery_pct DESC;


-- =====================================================
-- 2. DELIVERY TIME vs REVIEW SCORE CORRELATION
-- Skills: CASE bucketing, conditional aggregation
-- =====================================================

SELECT
    CASE
        WHEN o.delivery_delta_days >= 10 THEN '1. Very Early (10+ days early)'
        WHEN o.delivery_delta_days BETWEEN 1 AND 9 THEN '2. Early (1-9 days)'
        WHEN o.delivery_delta_days = 0 THEN '3. On Time'
        WHEN o.delivery_delta_days BETWEEN -5 AND -1 THEN '4. Slightly Late (1-5 days)'
        ELSE '5. Very Late (5+ days)'
    END AS delivery_bucket,
    COUNT(*) AS order_count,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    -- Breakdown by review score
    SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END) AS five_star,
    SUM(CASE WHEN r.review_score = 4 THEN 1 ELSE 0 END) AS four_star,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS bad_reviews,
    ROUND(SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS bad_review_pct
FROM fact_orders o
JOIN fact_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
  AND o.delivery_delta_days IS NOT NULL
GROUP BY delivery_bucket
ORDER BY delivery_bucket;


-- =====================================================
-- 3. MONTHLY DELIVERY PERFORMANCE TREND
-- Skills: CTE, Window Function (LAG for trend)
-- =====================================================

WITH monthly_delivery AS (
    SELECT
        o.order_month,
        COUNT(*) AS total_deliveries,
        ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days,
        SUM(o.is_late_delivery) AS late_count,
        ROUND(SUM(o.is_late_delivery) * 100.0 / COUNT(*), 2) AS late_pct
    FROM fact_orders o
    WHERE o.order_status = 'delivered'
      AND o.delivery_days_actual IS NOT NULL
    GROUP BY o.order_month
)
SELECT *,
    -- Is delivery getting better or worse month-over-month?
    LAG(avg_delivery_days) OVER (ORDER BY order_month) AS prev_month_avg_days,
    ROUND(avg_delivery_days - LAG(avg_delivery_days) OVER (ORDER BY order_month), 1)
        AS delivery_time_change,
    CASE
        WHEN avg_delivery_days < LAG(avg_delivery_days) OVER (ORDER BY order_month)
            THEN 'Improving'
        WHEN avg_delivery_days > LAG(avg_delivery_days) OVER (ORDER BY order_month)
            THEN 'Worsening'
        ELSE 'Stable'
    END AS trend
FROM monthly_delivery
ORDER BY order_month;


-- =====================================================
-- 4. FREIGHT COST ANALYSIS BY STATE & PRODUCT WEIGHT
-- Skills: JOINs across 3 tables, CASE, aggregation
-- =====================================================

SELECT
    c.customer_state,
    CASE
        WHEN p.product_weight_g <= 500 THEN 'Light (≤500g)'
        WHEN p.product_weight_g <= 2000 THEN 'Medium (500g-2kg)'
        WHEN p.product_weight_g <= 5000 THEN 'Heavy (2-5kg)'
        ELSE 'Very Heavy (5kg+)'
    END AS weight_bucket,
    COUNT(*) AS item_count,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight,
    ROUND(AVG(oi.price), 2) AS avg_price,
    ROUND(AVG(oi.freight_value) / NULLIF(AVG(oi.price), 0) * 100, 2)
        AS freight_as_pct_of_price,
    ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days
FROM fact_order_items oi
JOIN fact_orders o ON oi.order_id = o.order_id
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
  AND o.delivery_days_actual IS NOT NULL
  AND p.product_weight_g IS NOT NULL
GROUP BY c.customer_state, weight_bucket
ORDER BY c.customer_state, weight_bucket;


-- =====================================================
-- 5. SELLER DELIVERY RELIABILITY RANKING
-- Skills: CTE, Window Functions (DENSE_RANK, NTILE)
-- =====================================================

WITH seller_delivery AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days,
        SUM(o.is_late_delivery) AS late_deliveries,
        ROUND(SUM(o.is_late_delivery) * 100.0
              / COUNT(DISTINCT oi.order_id), 2) AS late_pct,
        ROUND(AVG(r.review_score), 2) AS avg_review
    FROM fact_order_items oi
    JOIN fact_orders o ON oi.order_id = o.order_id
    JOIN dim_sellers s ON oi.seller_id = s.seller_id
    LEFT JOIN fact_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
      AND o.delivery_days_actual IS NOT NULL
    GROUP BY s.seller_id, s.seller_city, s.seller_state
    HAVING COUNT(DISTINCT oi.order_id) >= 10   -- minimum volume filter
)
SELECT *,
    DENSE_RANK() OVER (ORDER BY late_pct ASC, avg_review DESC) AS reliability_rank,
    NTILE(4) OVER (ORDER BY late_pct ASC) AS reliability_quartile,
    CASE NTILE(4) OVER (ORDER BY late_pct ASC)
        WHEN 1 THEN 'Excellent'
        WHEN 2 THEN 'Good'
        WHEN 3 THEN 'Fair'
        WHEN 4 THEN 'Poor'
    END AS reliability_tier
FROM seller_delivery
ORDER BY reliability_rank
LIMIT 50;
