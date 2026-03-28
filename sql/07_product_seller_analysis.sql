-- =====================================================
-- E-Commerce BI Project - Product & Seller Analysis
-- Advanced SQL: Seller ranking, category cross-analysis
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- 1. SELLER PERFORMANCE RANKING (Revenue + Reviews)
-- Skills: CTE, Window Functions (DENSE_RANK, NTILE, cumulative SUM)
-- =====================================================

WITH seller_performance AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(oi.item_id) AS total_items_sold,
        ROUND(SUM(oi.total_value), 2) AS total_revenue,
        ROUND(AVG(oi.price), 2) AS avg_item_price,
        ROUND(AVG(oi.freight_value), 2) AS avg_freight,
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS bad_reviews
    FROM dim_sellers s
    JOIN fact_order_items oi ON s.seller_id = oi.seller_id
    JOIN fact_orders o ON oi.order_id = o.order_id
    LEFT JOIN fact_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY s.seller_id, s.seller_city, s.seller_state
)
SELECT *,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    NTILE(10) OVER (ORDER BY total_revenue DESC) AS revenue_decile,
    -- Revenue share
    ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 4) AS revenue_share_pct,
    -- Cumulative revenue (for Pareto)
    SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
    ROUND(SUM(total_revenue) OVER (ORDER BY total_revenue DESC)
          * 100.0 / SUM(total_revenue) OVER (), 2) AS cumulative_revenue_pct
FROM seller_performance
ORDER BY total_revenue DESC;


-- =====================================================
-- 2. PRODUCT CATEGORY PERFORMANCE MATRIX
-- Skills: JOINs, multi-metric aggregation, RANK
-- =====================================================

SELECT
    p.product_category_english AS category,
    COUNT(DISTINCT oi.order_id) AS order_count,
    COUNT(oi.item_id) AS items_sold,
    ROUND(SUM(oi.total_value), 2) AS total_revenue,
    ROUND(AVG(oi.price), 2) AS avg_price,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight,
    ROUND(AVG(r.review_score), 2) AS avg_review,
    ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days,
    ROUND(SUM(o.is_late_delivery) * 100.0 / COUNT(*), 2) AS late_delivery_pct,
    -- Rank across different metrics
    DENSE_RANK() OVER (ORDER BY SUM(oi.total_value) DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY AVG(r.review_score) DESC) AS satisfaction_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT oi.order_id) DESC) AS volume_rank
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
JOIN fact_orders o ON oi.order_id = o.order_id
LEFT JOIN fact_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
  AND p.product_category_english IS NOT NULL
GROUP BY p.product_category_english
ORDER BY total_revenue DESC;


-- =====================================================
-- 3. PRODUCT CATEGORY CROSS-SELLING (Basket Analysis)
-- Skills: Self-JOIN, CTE, aggregation
-- =====================================================

WITH order_categories AS (
    SELECT DISTINCT
        oi.order_id,
        p.product_category_english AS category
    FROM fact_order_items oi
    JOIN dim_products p ON oi.product_id = p.product_id
    WHERE p.product_category_english IS NOT NULL
)
SELECT
    a.category AS category_a,
    b.category AS category_b,
    COUNT(DISTINCT a.order_id) AS co_occurrence_count,
    ROUND(COUNT(DISTINCT a.order_id) * 100.0
          / (SELECT COUNT(DISTINCT order_id) FROM order_categories
             WHERE category = a.category), 2) AS pct_of_category_a_orders
FROM order_categories a
JOIN order_categories b
    ON a.order_id = b.order_id AND a.category < b.category
GROUP BY a.category, b.category
HAVING COUNT(DISTINCT a.order_id) >= 20   -- minimum co-occurrence threshold
ORDER BY co_occurrence_count DESC
LIMIT 20;


-- =====================================================
-- 4. SELLER STATE PERFORMANCE SUMMARY
-- Skills: GROUP BY, multi-metric, RANK
-- =====================================================

SELECT
    s.seller_state,
    COUNT(DISTINCT s.seller_id) AS seller_count,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    ROUND(SUM(oi.total_value), 2) AS total_revenue,
    ROUND(SUM(oi.total_value) / COUNT(DISTINCT s.seller_id), 2) AS revenue_per_seller,
    ROUND(AVG(r.review_score), 2) AS avg_review,
    ROUND(AVG(o.delivery_days_actual), 1) AS avg_delivery_days,
    DENSE_RANK() OVER (ORDER BY SUM(oi.total_value) DESC) AS state_rank
FROM dim_sellers s
JOIN fact_order_items oi ON s.seller_id = oi.seller_id
JOIN fact_orders o ON oi.order_id = o.order_id
LEFT JOIN fact_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_state
ORDER BY total_revenue DESC;


-- =====================================================
-- 5. TOP PRODUCTS — Highest Revenue Individual Products
-- Skills: JOINs, aggregation, RANK
-- =====================================================

SELECT
    p.product_id,
    p.product_category_english AS category,
    COUNT(DISTINCT oi.order_id) AS times_ordered,
    COUNT(oi.item_id) AS total_units_sold,
    ROUND(SUM(oi.total_value), 2) AS total_revenue,
    ROUND(AVG(oi.price), 2) AS avg_price,
    ROUND(AVG(r.review_score), 2) AS avg_review,
    DENSE_RANK() OVER (ORDER BY SUM(oi.total_value) DESC) AS product_rank
FROM fact_order_items oi
JOIN dim_products p ON oi.product_id = p.product_id
JOIN fact_orders o ON oi.order_id = o.order_id
LEFT JOIN fact_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_id, p.product_category_english
ORDER BY total_revenue DESC
LIMIT 30;
