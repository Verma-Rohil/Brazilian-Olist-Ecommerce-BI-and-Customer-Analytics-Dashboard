"""
E-Commerce BI Project - Cohort Retention Analysis
Monthly cohort analysis with retention heatmap

Usage:
    1. Run ETL pipeline first (01_etl_pipeline.py)
    2. Update MYSQL_CONFIG with your credentials
    3. Run: python python/05_cohort_analysis.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sqlalchemy import create_engine
import os
import warnings

warnings.filterwarnings('ignore')

# =====================================================
# CONFIGURATION
# =====================================================

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password123',
    'database': 'ecommerce_analytics',
    'port': 3306
}

ENGINE_URL = (
    f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
    f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
)

PLOTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)

plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams.update({
    'figure.dpi': 150, 'savefig.dpi': 150, 'savefig.bbox': 'tight',
    'font.size': 12, 'axes.titlesize': 16, 'axes.labelsize': 13
})


def get_engine():
    return create_engine(ENGINE_URL, echo=False)


def save_plot(fig, filename):
    path = os.path.join(PLOTS_DIR, filename)
    fig.savefig(path)
    plt.close(fig)
    print(f"  ✓ Saved: {filename}")


# =====================================================
# STEP 1: EXTRACT COHORT DATA
# =====================================================

def get_cohort_data(engine):
    """Pull order data with customer's first purchase (cohort) information."""
    print("\n  [Step 1] Extracting cohort data from MySQL...")

    query = """
    WITH first_purchase AS (
        SELECT
            c.customer_unique_id,
            MIN(o.order_month) AS cohort_month
        FROM dim_customers c
        JOIN fact_orders o ON c.customer_id = o.customer_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_unique_id
    )
    SELECT
        fp.customer_unique_id,
        fp.cohort_month,
        o.order_month
    FROM first_purchase fp
    JOIN dim_customers c ON fp.customer_unique_id = c.customer_unique_id
    JOIN fact_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    """
    from sqlalchemy import text
    df = pd.read_sql(text(query), engine)
    print(f"    → {df.shape[0]:,} order records for {df['customer_unique_id'].nunique():,} unique customers")
    print(f"    → Cohort range: {df['cohort_month'].min()} to {df['cohort_month'].max()}")
    return df


# =====================================================
# STEP 2: BUILD RETENTION MATRIX
# =====================================================

def build_retention_matrix(df):
    """Build cohort retention matrix."""
    print("\n  [Step 2] Building retention matrix...")

    # Convert to period for month arithmetic
    df['cohort_month_dt'] = pd.to_datetime(df['cohort_month'] + '-01')
    df['order_month_dt'] = pd.to_datetime(df['order_month'] + '-01')

    # Calculate months since first purchase
    df['months_since_first'] = (
        (df['order_month_dt'].dt.year - df['cohort_month_dt'].dt.year) * 12
        + (df['order_month_dt'].dt.month - df['cohort_month_dt'].dt.month)
    )

    # Count unique customers per (cohort, months_since_first)
    cohort_data = df.groupby(['cohort_month', 'months_since_first'])\
        .agg(active_customers=('customer_unique_id', 'nunique'))\
        .reset_index()

    # Cohort sizes (Month 0)
    cohort_sizes = cohort_data[cohort_data['months_since_first'] == 0]\
        .set_index('cohort_month')['active_customers']

    # Build retention matrix (pivot)
    retention_counts = cohort_data.pivot_table(
        index='cohort_month',
        columns='months_since_first',
        values='active_customers',
        fill_value=0
    )

    # Calculate retention percentage
    retention_pct = retention_counts.divide(cohort_sizes, axis=0) * 100

    print(f"    → {len(cohort_sizes)} cohorts created")
    print(f"    → Max months tracked: {retention_pct.columns.max()}")

    return retention_counts, retention_pct, cohort_sizes


# =====================================================
# STEP 3: COHORT RETENTION HEATMAP
# =====================================================

def plot_retention_heatmap(retention_pct, cohort_sizes):
    """Main deliverable: cohort retention heatmap."""
    print("\n  [Step 3] Generating retention heatmap...")

    # Filter to show first 12 months and cohorts with reasonable size
    max_months = min(12, retention_pct.columns.max())
    display_pct = retention_pct.loc[:, 0:max_months].copy()

    # Filter cohorts with at least 50 customers
    valid_cohorts = cohort_sizes[cohort_sizes >= 50].index
    display_pct = display_pct.loc[display_pct.index.isin(valid_cohorts)]

    if display_pct.empty:
        print("    ⚠️ Not enough data for heatmap")
        return

    fig, ax = plt.subplots(figsize=(16, max(8, len(display_pct) * 0.5)))

    # Custom annotation: show % and add cohort size to index
    annot_labels = display_pct.round(1).astype(str) + '%'
    annot_labels = annot_labels.replace('0.0%', '')

    sns.heatmap(
        display_pct,
        annot=annot_labels,
        fmt='',
        cmap='YlGnBu',
        linewidths=0.5,
        linecolor='white',
        ax=ax,
        vmin=0,
        vmax=100,
        cbar_kws={'label': 'Retention Rate (%)', 'shrink': 0.8}
    )

    # Annotate cohort sizes on y-axis
    new_labels = [f"{cohort}\n(n={cohort_sizes.get(cohort, 0):,})"
                  for cohort in display_pct.index]
    ax.set_yticklabels(new_labels, rotation=0)

    ax.set_xlabel('Months Since First Purchase', fontsize=13)
    ax.set_ylabel('Cohort (First Purchase Month)', fontsize=13)
    ax.set_title('Customer Cohort Retention Analysis',
                 fontsize=18, fontweight='bold', pad=20)

    fig.tight_layout()
    save_plot(fig, 'cohort_retention_heatmap.png')


# =====================================================
# STEP 4: COHORT SIZE BAR CHART
# =====================================================

def plot_cohort_sizes(cohort_sizes):
    """Bar chart: number of customers per cohort."""
    print("  [Step 4] Cohort size bar chart...")

    fig, ax = plt.subplots(figsize=(14, 6))

    colors = sns.color_palette('viridis', len(cohort_sizes))
    bars = ax.bar(range(len(cohort_sizes)), cohort_sizes.values,
                  color=colors, edgecolor='white', linewidth=0.5)

    # Label top of bars
    for bar, val in zip(bars, cohort_sizes.values):
        if val >= 100:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                    f'{val:,}', ha='center', fontsize=8, rotation=45)

    ax.set_xticks(range(len(cohort_sizes)))
    ax.set_xticklabels(cohort_sizes.index, rotation=45, ha='right', fontsize=9)
    ax.set_xlabel('Cohort Month')
    ax.set_ylabel('Number of New Customers')
    ax.set_title('New Customer Acquisition by Cohort Month',
                 fontsize=16, fontweight='bold', pad=15)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))

    fig.tight_layout()
    save_plot(fig, 'cohort_size_bar.png')


# =====================================================
# STEP 5: AVERAGE RETENTION CURVE
# =====================================================

def plot_retention_curve(retention_pct):
    """Line chart: average retention across all cohorts."""
    print("  [Step 5] Average retention curve...")

    max_months = min(12, retention_pct.columns.max())
    avg_retention = retention_pct.loc[:, 0:max_months].mean(axis=0)

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(avg_retention.index, avg_retention.values,
            'o-', color='#2E86AB', linewidth=2.5, markersize=10,
            markerfacecolor='white', markeredgewidth=2.5)

    # Fill under curve
    ax.fill_between(avg_retention.index, avg_retention.values,
                    alpha=0.15, color='#2E86AB')

    # Annotate each point
    for month, pct in avg_retention.items():
        if pct > 0:
            ax.annotate(f'{pct:.1f}%',
                       xy=(month, pct),
                       xytext=(0, 15),
                       textcoords='offset points',
                       ha='center', fontsize=10, fontweight='bold')

    ax.set_xlabel('Months Since First Purchase')
    ax.set_ylabel('Average Retention Rate (%)')
    ax.set_title('Average Customer Retention Curve',
                 fontsize=16, fontweight='bold', pad=15)
    ax.set_xticks(range(int(max_months) + 1))
    ax.set_ylim(0, 105)
    ax.grid(True, alpha=0.3)

    # Add key insight
    if len(avg_retention) > 1:
        month1_retention = avg_retention.get(1, 0)
        ax.text(0.95, 0.95,
                f'Month 1 Retention: {month1_retention:.1f}%',
                transform=ax.transAxes, fontsize=13, fontweight='bold',
                ha='right', va='top',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightyellow',
                         edgecolor='orange'))

    fig.tight_layout()
    save_plot(fig, 'retention_curve.png')

    return avg_retention


# =====================================================
# STEP 6: SUMMARY INSIGHTS
# =====================================================

def print_insights(retention_pct, cohort_sizes, avg_retention):
    """Print key cohort analysis insights."""
    print("\n" + "=" * 60)
    print("  COHORT ANALYSIS — KEY INSIGHTS")
    print("=" * 60)

    total_customers = cohort_sizes.sum()
    largest_cohort = cohort_sizes.idxmax()
    largest_size = cohort_sizes.max()

    print(f"""
    📊 Overview:
    ├── Total unique customers: {total_customers:,}
    ├── Number of cohorts:      {len(cohort_sizes)}
    ├── Largest cohort:         {largest_cohort} ({largest_size:,} customers)
    └── Smallest cohort:        {cohort_sizes.idxmin()} ({cohort_sizes.min():,} customers)
    """)

    if len(avg_retention) > 1:
        m1 = avg_retention.get(1, 0)
        m3 = avg_retention.get(3, 0)
        m6 = avg_retention.get(6, 0)
        print(f"""    📉 Retention Summary:
    ├── Month 1 avg retention:  {m1:.1f}%
    ├── Month 3 avg retention:  {m3:.1f}%
    └── Month 6 avg retention:  {m6:.1f}%

    💡 BUSINESS INSIGHT:
    Most customers do not return after their first purchase.
    Month 1 retention of {m1:.1f}% suggests opportunity for:
    • Post-purchase email campaigns
    • Loyalty/rewards program to incentivize repeat purchases
    • Personalized product recommendations
    """)


# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 60)
    print("  E-COMMERCE BI PROJECT — COHORT RETENTION ANALYSIS")
    print("=" * 60)

    engine = get_engine()

    # Step 1: Get data
    df = get_cohort_data(engine)

    # Step 2: Build matrix
    retention_counts, retention_pct, cohort_sizes = build_retention_matrix(df)

    # Step 3: Heatmap
    plot_retention_heatmap(retention_pct, cohort_sizes)

    # Step 4: Cohort sizes
    plot_cohort_sizes(cohort_sizes)

    # Step 5: Retention curve
    avg_retention = plot_retention_curve(retention_pct)

    # Step 6: Insights
    print_insights(retention_pct, cohort_sizes, avg_retention)

    print("=" * 60)
    print("  ✅ COHORT ANALYSIS COMPLETE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
