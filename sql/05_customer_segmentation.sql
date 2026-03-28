-- =====================================================
-- E-Commerce BI Project - Customer Segmentation
-- Advanced SQL: RFM Scoring, Cohort Retention
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- 1. RFM SCORING — Recency, Frequency, Monetary
-- Skills: CTE, NTILE Window Function, CASE segmentation
-- =====================================================

WITH customer_metrics AS (
    SELECT
        c.customer_unique_id,
        -- Recency: days since last order (from max date in dataset)
        DATEDIFF(
            (SELECT MAX(order_purchase_timestamp) FROM fact_orders),
            MAX(o.order_purchase_timestamp)
        ) AS recency_days,
        -- Frequency: total number of orders
        COUNT(DISTINCT o.order_id) AS frequency,
        -- Monetary: total spending
        ROUND(SUM(oi.total_value), 2) AS monetary
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT *,
        -- Score each dimension 1-5 (5 = best)
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)       AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)         AS m_score
    FROM customer_metrics
)
SELECT *,
    CONCAT(r_score, f_score, m_score) AS rfm_segment_code,
    (r_score + f_score + m_score) AS rfm_total,
    -- Business-friendly segment labels
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score >= 3                   THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2                   THEN 'New Customers'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3  THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND f_score >= 3                   THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3  THEN 'Cant Lose Them'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2  THEN 'Lost'
        ELSE 'Need Attention'
    END AS customer_segment
FROM rfm_scores
ORDER BY rfm_total DESC;


-- =====================================================
-- 2. RFM SEGMENT SUMMARY — Aggregate stats per segment
-- Skills: subquery / CTE reuse, aggregation
-- =====================================================

WITH customer_metrics AS (
    SELECT
        c.customer_unique_id,
        DATEDIFF(
            (SELECT MAX(order_purchase_timestamp) FROM fact_orders),
            MAX(o.order_purchase_timestamp)
        ) AS recency_days,
        COUNT(DISTINCT o.order_id) AS frequency,
        ROUND(SUM(oi.total_value), 2) AS monetary
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)        AS m_score
    FROM customer_metrics
),
segmented AS (
    SELECT *,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 4 AND f_score >= 3                   THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                   THEN 'New Customers'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3  THEN 'Potential Loyalists'
            WHEN r_score <= 2 AND f_score >= 3                   THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3  THEN 'Cant Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2  THEN 'Lost'
            ELSE 'Need Attention'
        END AS customer_segment
    FROM rfm_scores
)
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
    ROUND(SUM(monetary), 2) AS total_revenue,
    ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2) AS revenue_share_pct,
    ROUND(AVG(recency_days), 0) AS avg_recency,
    ROUND(AVG(frequency), 1) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary
FROM segmented
GROUP BY customer_segment
ORDER BY total_revenue DESC;


-- =====================================================
-- 3. COHORT RETENTION ANALYSIS
-- Skills: CTE, PERIOD_DIFF, FIRST_VALUE window, pivot-style
-- =====================================================

WITH first_purchase AS (
    -- Identify each customer's cohort (first purchase month)
    SELECT
        c.customer_unique_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
subsequent_purchases AS (
    -- Map every order to its cohort and months-since-first
    SELECT
        fp.customer_unique_id,
        fp.cohort_month,
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
        PERIOD_DIFF(
            EXTRACT(YEAR_MONTH FROM o.order_purchase_timestamp),
            EXTRACT(YEAR_MONTH FROM STR_TO_DATE(CONCAT(fp.cohort_month, '-01'), '%Y-%m-%d'))
        ) AS months_since_first
    FROM first_purchase fp
    JOIN dim_customers c ON fp.customer_unique_id = c.customer_unique_id
    JOIN fact_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
),
cohort_data AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM subsequent_purchases
    GROUP BY cohort_month, months_since_first
)
SELECT
    cd.cohort_month,
    cd.months_since_first,
    cd.active_customers,
    -- Cohort size (Month 0 count)
    FIRST_VALUE(cd.active_customers) OVER (
        PARTITION BY cd.cohort_month ORDER BY cd.months_since_first
    ) AS cohort_size,
    -- Retention percentage
    ROUND(
        cd.active_customers * 100.0
        / FIRST_VALUE(cd.active_customers) OVER (
            PARTITION BY cd.cohort_month ORDER BY cd.months_since_first
        ), 2
    ) AS retention_pct
FROM cohort_data cd
ORDER BY cd.cohort_month, cd.months_since_first;


-- =====================================================
-- 4. CUSTOMER LIFETIME VALUE (CLV) ESTIMATION
-- Skills: CTE, aggregation, NTILE for tiering
-- =====================================================

WITH customer_clv AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(SUM(oi.total_value), 2) AS total_revenue,
        ROUND(AVG(oi.total_value), 2) AS avg_order_value,
        DATEDIFF(MAX(o.order_purchase_timestamp),
                 MIN(o.order_purchase_timestamp)) AS customer_lifespan_days,
        MIN(o.order_purchase_timestamp) AS first_order,
        MAX(o.order_purchase_timestamp) AS last_order
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)
SELECT *,
    -- CLV tier
    NTILE(5) OVER (ORDER BY total_revenue ASC) AS clv_tier,
    CASE NTILE(5) OVER (ORDER BY total_revenue ASC)
        WHEN 5 THEN 'Platinum'
        WHEN 4 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        WHEN 2 THEN 'Bronze'
        WHEN 1 THEN 'Basic'
    END AS clv_label
FROM customer_clv
ORDER BY total_revenue DESC;
