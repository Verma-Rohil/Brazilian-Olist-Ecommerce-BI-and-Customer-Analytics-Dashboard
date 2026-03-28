# 🛒 E-Commerce Business Intelligence & Customer Analytics Platform

## 📋 Overview

End-to-end BI platform analyzing **100K+ real e-commerce orders** from Brazil's Olist marketplace (2016–2018). Covers customer segmentation (RFM + K-Means), revenue analytics, delivery SLA tracking, product/seller performance, and executive dashboards.

**Dataset:** [Brazilian Olist E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 8 relational tables covering orders, items, payments, reviews, products, sellers, customers, and geolocation.

---

## 🎯 Business Objectives

- **Customer Segmentation:** Identify high-value segments using RFM analysis and K-Means clustering
- **Revenue Analytics:** Track monthly trends, MoM growth, and Pareto analysis
- **Delivery SLA Monitoring:** Analyze delivery performance and its impact on satisfaction
- **Product & Seller Performance:** Rank sellers by reliability, identify cross-selling opportunities
- **Cohort Retention:** Build monthly cohort retention analysis to understand customer lifecycle

---

## 🛠️ Tech Stack

| Tool | Usage |
|------|-------|
| **MySQL 8.0** | Star schema (4 dim + 4 fact tables), 22 advanced queries with CTEs, Window Functions |
| **Python 3.10+** | ETL pipeline, K-Means clustering, statistical hypothesis testing, cohort analysis |
| **Pandas, NumPy, SciPy** | Data manipulation, Welch's t-test, Pearson/Spearman correlation |
| **Matplotlib, Seaborn** | 20 publication-quality visualizations |
| **Scikit-learn** | K-Means clustering, StandardScaler, silhouette analysis |
| **Power BI** | 4-page executive dashboard with 15+ DAX measures |

---

## 📊 Key Findings

### Customer Segmentation (K-Means, K=2, Silhouette = 0.71)
- **Champions:** 2,801 customers (3%) — avg spend R$309, frequency 2.1 orders
- **Loyal Customers:** 90,557 customers (97%) — avg spend R$161, frequency 1.0 orders
- Champions drive **5.6%** of total revenue despite being only 3% of customers

### Delivery Impact (Welch's t-test, p < 0.001, Cohen's d = 1.13)
- On-time deliveries average **4.21★** review score vs **2.56★** for late deliveries
- This is a **large effect size** — late delivery is the #1 driver of poor reviews
- Delivery days negatively correlated with review score (r = -0.31)

### Revenue Concentration
- Top 10 product categories account for the majority of revenue
- São Paulo (SP) dominates with highest order volume and revenue

### Cohort Retention (23 cohorts, 2016-09 to 2018-08)
- Month-1 retention: **4.7%** — most customers are one-time buyers
- Month-3 retention drops to **0.2%**
- Largest cohort: November 2017 (7,060 customers) — likely Black Friday effect
- **Recommendation:** Post-purchase email campaigns, loyalty programs, personalized recommendations

---

## 📂 Project Structure

```
ecommerce_bi_project/
├── data/
│   ├── raw/                           # 8 Olist CSV files
│   └── processed/
├── sql/
│   ├── 01_schema_design.sql           # Star schema (4 dim + 4 fact, 15 indexes)
│   ├── 03_eda_queries.sql             # 8 baseline KPI queries
│   ├── 04_revenue_analytics.sql       # Waterfall, Pareto, moving avg, payment
│   ├── 05_customer_segmentation.sql   # RFM scoring, cohort retention, CLV
│   ├── 06_delivery_logistics.sql      # SLA, delivery-review, freight analysis
│   └── 07_product_seller_analysis.sql # Seller ranking, basket analysis
├── python/
│   ├── 01_etl_pipeline.py             # CSV → MySQL ETL (93K customers loaded)
│   ├── 02_eda_analysis.py             # 8 EDA visualizations
│   ├── 03_rfm_segmentation.py         # K-Means + 4 plots + MySQL export
│   ├── 04_correlation_analysis.py     # 3 hypothesis tests + 4 plots
│   └── 05_cohort_analysis.py          # Retention heatmap + curve + insights
├── plots/                             # 20 generated visualizations
├── powerbi/
│   └── ecommerce_bi_dashboard.pbix    # 4-page Power BI dashboard
├── docs/
│   ├── README.md
│   └── powerbi_guide.md              # Dashboard build guide with DAX
├── requirements.txt
└── .gitignore
```

---

## 🚀 How to Run

### Prerequisites
- MySQL 8.0+ installed and running
- Python 3.10+ with pip
- Power BI Desktop (for dashboard)

### Step-by-Step

1. **Download the dataset** from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files in `data/raw/`

2. **Set up Python environment:**
   ```bash
   cd ecommerce_bi_project
   python -m venv venv
   venv\Scripts\activate
   pip install pandas numpy scipy matplotlib seaborn mysql-connector-python sqlalchemy scikit-learn openpyxl
   ```

3. **Create the MySQL database:**
   - Open MySQL Workbench
   - Run `sql/01_schema_design.sql` to create the database and tables

4. **Run the ETL pipeline:**
   - Update `MYSQL_CONFIG` in `python/01_etl_pipeline.py` with your MySQL password
   - Run: `python python/01_etl_pipeline.py`

5. **Run SQL analytics** (in MySQL Workbench):
   - Execute files 03-07 in the `sql/` folder

6. **Run Python analytics:**
   ```bash
   python python/02_eda_analysis.py
   python python/03_rfm_segmentation.py
   python python/04_correlation_analysis.py
   python python/05_cohort_analysis.py
   ```

7. **Build Power BI dashboard:**
   - Connect to MySQL `ecommerce_analytics` database
   - Follow `docs/powerbi_guide.md` for DAX measures and page layouts

---

## 📸 Sample Visualizations

| Monthly Revenue Trend | Cohort Retention Heatmap | RFM Cluster Analysis |
|:---:|:---:|:---:|
| Revenue & order volume trend over 24 months | 23 cohorts with month-over-month retention rates | K-Means scatter plot of customer segments |

| Correlation Heatmap | Delivery Impact on Reviews | Day × Hour Heatmap |
|:---:|:---:|:---:|
| Pearson & Spearman dual heatmaps | On-time (4.21★) vs Late (2.56★) boxplot | Order patterns by weekday and hour |

---

## 🏆 Skills Demonstrated

| Skill | Evidence |
|-------|----------|
| **Advanced SQL** | CTEs, Window Functions (NTILE, LAG, DENSE_RANK, running totals), multi-table JOINs, CASE |
| **Python ETL** | Automated CSV → MySQL pipeline with data validation & feature engineering |
| **Machine Learning** | K-Means clustering (K=2, Silhouette=0.71), elbow method, StandardScaler |
| **Statistical Analysis** | Welch's t-test (Cohen's d=1.13), Pearson/Spearman correlation, p-values |
| **Data Visualization** | 20 matplotlib/seaborn charts + 4-page Power BI dashboard with 15 DAX measures |
| **Data Modeling** | Star schema (4 dim + 4 fact tables), 15 performance indexes |
| **Business Intelligence** | RFM segmentation, Pareto analysis, cohort retention, CLV estimation |

---

## 💼 Resume Impact Statement

> **Built an end-to-end E-Commerce BI platform** analyzing 100K+ orders using MySQL (star schema, 22 advanced queries with CTEs & window functions), Python (K-Means clustering with 0.71 silhouette score, 3 hypothesis tests at p<0.001), and Power BI (4-page dashboard with 15 DAX measures), **identifying that late deliveries reduce review scores by 39% (Cohen's d=1.13)** and that Month-1 customer retention is only 4.7%, driving actionable recommendations for loyalty programs.
