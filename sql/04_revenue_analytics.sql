-- =====================================================
-- E-Commerce BI Project - Revenue Analytics
-- Advanced SQL: CTEs, Window Functions, Pareto Analysis
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- 1. REVENUE WATERFALL — Monthly Revenue + Running Total + MoM Growth
-- Skills: CTE, Window Functions (SUM OVER, LAG)
-- =====================================================

WITH monthly_revenue AS (
    SELECT
        o.order_month,
        COUNT(DISTINCT o.order_id)      AS total_orders,
        ROUND(SUM(oi.total_value), 2)   AS monthly_revenue,
        COUNT(DISTINCT o.customer_id)   AS unique_customers
    FROM fact_orders o
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_month
)
SELECT
    order_month,
    total_orders,
    monthly_revenue,
    unique_customers,
    ROUND(monthly_revenue / total_orders, 2) AS avg_order_value,
    -- Running total (cumulative revenue)
    SUM(monthly_revenue) OVER (ORDER BY order_month) AS running_total_revenue,
    -- Month-over-Month growth percentage
    LAG(monthly_revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_month))
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY order_month), 0) * 100
    , 2) AS mom_growth_pct
FROM monthly_revenue
ORDER BY order_month;


-- =====================================================
-- 2. PARETO ANALYSIS — Revenue by Product Category (80/20 Rule)
-- Skills: CTE, Window Functions (SUM OVER, ROW_NUMBER), CASE
-- =====================================================

WITH category_revenue AS (
    SELECT
        p.product_category_english AS category,
        ROUND(SUM(oi.total_value), 2) AS revenue,
        COUNT(DISTINCT oi.order_id) AS order_count,
        COUNT(oi.item_id) AS items_sold
    FROM fact_order_items oi
    JOIN dim_products p ON oi.product_id = p.product_id
    JOIN fact_orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
      AND p.product_category_english IS NOT NULL
    GROUP BY p.product_category_english
),
ranked AS (
    SELECT *,
        -- Percentage of total revenue
        ROUND(revenue / SUM(revenue) OVER () * 100, 2) AS revenue_pct,
        -- Cumulative percentage
        ROUND(SUM(revenue) OVER (ORDER BY revenue DESC)
              / SUM(revenue) OVER () * 100, 2) AS cumulative_pct,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rank_num
    FROM category_revenue
)
SELECT *,
    -- ABC classification
    CASE
        WHEN cumulative_pct <= 80 THEN 'A (Top 80%)'
        WHEN cumulative_pct <= 95 THEN 'B (Next 15%)'
        ELSE 'C (Bottom 5%)'
    END AS abc_class
FROM ranked
ORDER BY rank_num;


-- =====================================================
-- 3. AVERAGE ORDER VALUE BY PAYMENT TYPE & INSTALLMENTS
-- Skills: CTE, CASE bucketing, Window Function (SUM OVER)
-- =====================================================

WITH payment_analysis AS (
    SELECT
        p.payment_type,
        CASE
            WHEN p.payment_installments = 1 THEN '1 (Full Payment)'
            WHEN p.payment_installments BETWEEN 2 AND 3 THEN '2-3 Installments'
            WHEN p.payment_installments BETWEEN 4 AND 6 THEN '4-6 Installments'
            WHEN p.payment_installments BETWEEN 7 AND 10 THEN '7-10 Installments'
            ELSE '10+ Installments'
        END AS installment_bucket,
        p.payment_value,
        o.order_id
    FROM fact_payments p
    JOIN fact_orders o ON p.order_id = o.order_id
    WHERE o.order_status = 'delivered'
)
SELECT
    payment_type,
    installment_bucket,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(AVG(payment_value), 2) AS avg_payment_value,
    ROUND(SUM(payment_value), 2) AS total_payment_value,
    ROUND(COUNT(DISTINCT order_id) * 100.0
          / SUM(COUNT(DISTINCT order_id)) OVER (), 2) AS pct_of_orders
FROM payment_analysis
GROUP BY payment_type, installment_bucket
ORDER BY payment_type, avg_payment_value DESC;


-- =====================================================
-- 4. DAILY REVENUE TREND WITH 7-DAY MOVING AVERAGE
-- Skills: Window Function (AVG OVER with ROWS BETWEEN)
-- =====================================================

WITH daily_revenue AS (
    SELECT
        DATE(o.order_purchase_timestamp) AS order_date,
        COUNT(DISTINCT o.order_id) AS daily_orders,
        ROUND(SUM(oi.total_value), 2) AS daily_revenue
    FROM fact_orders o
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE(o.order_purchase_timestamp)
)
SELECT
    order_date,
    daily_orders,
    daily_revenue,
    -- 7-day moving average (smoothed trend line)
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_7d,
    -- Cumulative revenue
    SUM(daily_revenue) OVER (ORDER BY order_date) AS cumulative_revenue
FROM daily_revenue
ORDER BY order_date;


-- =====================================================
-- 5. REVENUE CONCENTRATION — Top 20% Customers vs Bottom 80%
-- Skills: CTE, NTILE, Aggregation over segments
-- =====================================================

WITH customer_spend AS (
    SELECT
        c.customer_unique_id,
        ROUND(SUM(oi.total_value), 2) AS total_spend,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
quintiles AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY total_spend DESC) AS spend_quintile
    FROM customer_spend
)
SELECT
    spend_quintile,
    CASE spend_quintile
        WHEN 1 THEN 'Top 20% (Quintile 1)'
        WHEN 2 THEN 'Q2 (21-40%)'
        WHEN 3 THEN 'Q3 (41-60%)'
        WHEN 4 THEN 'Q4 (61-80%)'
        WHEN 5 THEN 'Bottom 20% (Q5)'
    END AS segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spend), 2) AS segment_revenue,
    ROUND(AVG(total_spend), 2) AS avg_spend,
    ROUND(SUM(total_spend) * 100.0
          / (SELECT SUM(total_spend) FROM customer_spend), 2) AS revenue_share_pct
FROM quintiles
GROUP BY spend_quintile
ORDER BY spend_quintile;
