"""
E-Commerce BI Project - Exploratory Data Analysis
Generates 8 publication-quality visualizations

Usage:
    1. Run ETL pipeline first (01_etl_pipeline.py)
    2. Update MYSQL_CONFIG with your credentials
    3. Run: python python/02_eda_analysis.py
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

# Set global style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette('viridis')
plt.rcParams.update({
    'figure.figsize': (12, 7),
    'font.size': 12,
    'axes.titlesize': 16,
    'axes.labelsize': 13,
    'figure.dpi': 150,
    'savefig.bbox': 'tight',
    'savefig.dpi': 150
})


def get_engine():
    return create_engine(ENGINE_URL, echo=False)


def save_plot(fig, filename):
    """Save figure to plots directory."""
    path = os.path.join(PLOTS_DIR, filename)
    fig.savefig(path)
    plt.close(fig)
    print(f"  ✓ Saved: {filename}")


# =====================================================
# PLOT 1: Monthly Revenue Trend
# =====================================================

def plot_monthly_revenue(engine):
    """Line chart: Monthly revenue with order count overlay."""
    print("\n  [1/8] Monthly Revenue Trend...")

    query = """
    SELECT
        o.order_month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(SUM(oi.total_value), 2) AS revenue
    FROM fact_orders o
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_month
    ORDER BY o.order_month
    """
    df = pd.read_sql(query, engine)

    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Revenue line
    color_rev = '#2E86AB'
    ax1.plot(df['order_month'], df['revenue'], color=color_rev,
             linewidth=2.5, marker='o', markersize=6, label='Revenue (R$)')
    ax1.fill_between(range(len(df)), df['revenue'], alpha=0.1, color=color_rev)
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Revenue (R$)', color=color_rev)
    ax1.tick_params(axis='y', labelcolor=color_rev)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R${x:,.0f}'))

    # Order count on secondary axis
    ax2 = ax1.twinx()
    color_ord = '#E8630A'
    ax2.bar(range(len(df)), df['total_orders'], alpha=0.3, color=color_ord, label='Orders')
    ax2.set_ylabel('Order Count', color=color_ord)
    ax2.tick_params(axis='y', labelcolor=color_ord)

    plt.title('Monthly Revenue & Order Volume Trend', fontsize=18, fontweight='bold', pad=20)
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels(df['order_month'], rotation=45, ha='right')

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    fig.tight_layout()
    save_plot(fig, 'monthly_revenue_trend.png')


# =====================================================
# PLOT 2: Order Status Distribution
# =====================================================

def plot_order_status(engine):
    """Horizontal bar: Order count by status."""
    print("  [2/8] Order Status Distribution...")

    query = """
    SELECT order_status, COUNT(*) AS count
    FROM fact_orders
    GROUP BY order_status
    ORDER BY count DESC
    """
    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = sns.color_palette('RdYlGn_r', len(df))
    bars = ax.barh(df['order_status'], df['count'], color=colors, edgecolor='white')

    # Add value labels
    for bar, val in zip(bars, df['count']):
        ax.text(bar.get_width() + 200, bar.get_y() + bar.get_height() / 2,
                f'{val:,}', va='center', fontweight='bold')

    ax.set_xlabel('Number of Orders')
    ax.set_title('Order Status Distribution', fontsize=18, fontweight='bold', pad=15)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))

    fig.tight_layout()
    save_plot(fig, 'order_status_distribution.png')


# =====================================================
# PLOT 3: Top 10 Categories by Revenue
# =====================================================

def plot_top_categories(engine):
    """Horizontal bar: Top 10 product categories."""
    print("  [3/8] Top 10 Categories by Revenue...")

    query = """
    SELECT
        p.product_category_english AS category,
        ROUND(SUM(oi.total_value), 2) AS revenue
    FROM fact_order_items oi
    JOIN dim_products p ON oi.product_id = p.product_id
    JOIN fact_orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
      AND p.product_category_english IS NOT NULL
    GROUP BY p.product_category_english
    ORDER BY revenue DESC
    LIMIT 10
    """
    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots(figsize=(12, 7))
    colors = sns.color_palette('viridis', len(df))
    bars = ax.barh(df['category'], df['revenue'], color=colors, edgecolor='white')

    for bar, val in zip(bars, df['revenue']):
        ax.text(bar.get_width() + 5000, bar.get_y() + bar.get_height() / 2,
                f'R${val:,.0f}', va='center', fontweight='bold', fontsize=10)

    ax.set_xlabel('Total Revenue (R$)')
    ax.set_title('Top 10 Product Categories by Revenue', fontsize=18, fontweight='bold', pad=15)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R${x:,.0f}'))

    fig.tight_layout()
    save_plot(fig, 'top_categories_revenue.png')


# =====================================================
# PLOT 4: Payment Type Distribution
# =====================================================

def plot_payment_types(engine):
    """Donut chart: Payment type split."""
    print("  [4/8] Payment Type Distribution...")

    query = """
    SELECT payment_type, COUNT(*) AS count
    FROM fact_payments
    GROUP BY payment_type
    ORDER BY count DESC
    """
    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots(figsize=(9, 9))
    colors = ['#2E86AB', '#E8630A', '#44AF69', '#F8C630', '#D64045']
    wedges, texts, autotexts = ax.pie(
        df['count'], labels=df['payment_type'], autopct='%1.1f%%',
        colors=colors[:len(df)], startangle=90, pctdistance=0.82,
        wedgeprops=dict(width=0.45, edgecolor='white', linewidth=2)
    )

    for text in autotexts:
        text.set_fontweight('bold')
        text.set_fontsize(11)

    ax.set_title('Payment Type Distribution', fontsize=18, fontweight='bold', pad=20)

    # Center label
    ax.text(0, 0, f'{df["count"].sum():,}\nTotal',
            ha='center', va='center', fontsize=16, fontweight='bold', color='#333')

    fig.tight_layout()
    save_plot(fig, 'payment_type_distribution.png')


# =====================================================
# PLOT 5: Review Score Distribution
# =====================================================

def plot_review_scores(engine):
    """Bar chart: Review score frequency with percentages."""
    print("  [5/8] Review Score Distribution...")

    query = """
    SELECT review_score, COUNT(*) AS count
    FROM fact_reviews
    GROUP BY review_score
    ORDER BY review_score
    """
    df = pd.read_sql(query, engine)
    df['pct'] = df['count'] / df['count'].sum() * 100

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = ['#D64045', '#E8630A', '#F8C630', '#96C93D', '#44AF69']
    bars = ax.bar(df['review_score'].astype(str), df['count'], color=colors,
                  edgecolor='white', linewidth=1.5, width=0.6)

    for bar, pct in zip(bars, df['pct']):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
                f'{pct:.1f}%', ha='center', fontweight='bold', fontsize=12)

    ax.set_xlabel('Review Score (1-5 ★)')
    ax.set_ylabel('Number of Reviews')
    ax.set_title('Customer Review Score Distribution', fontsize=18, fontweight='bold', pad=15)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))

    # Add mean line
    mean_score = (df['review_score'] * df['count']).sum() / df['count'].sum()
    ax.axhline(y=ax.get_ylim()[1] * 0.5, color='red', linestyle='--', alpha=0)  # invisible spacer
    ax.text(0.95, 0.95, f'Mean: {mean_score:.2f} ★', transform=ax.transAxes,
            fontsize=14, fontweight='bold', ha='right', va='top',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='lightyellow', edgecolor='orange'))

    fig.tight_layout()
    save_plot(fig, 'review_score_distribution.png')


# =====================================================
# PLOT 6: Delivery Time Box Plot by State (Top 10)
# =====================================================

def plot_delivery_by_state(engine):
    """Box plot: Delivery days distribution by top 10 states."""
    print("  [6/8] Delivery Time by State...")

    query = """
    SELECT c.customer_state, o.delivery_days_actual
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND o.delivery_days_actual IS NOT NULL
      AND o.delivery_days_actual BETWEEN 0 AND 60
    """
    df = pd.read_sql(query, engine)

    # Top 10 states by order volume
    top_states = df['customer_state'].value_counts().head(10).index.tolist()
    df_top = df[df['customer_state'].isin(top_states)]

    # Sort by median delivery time
    state_order = df_top.groupby('customer_state')['delivery_days_actual'].median()\
                        .sort_values(ascending=False).index

    fig, ax = plt.subplots(figsize=(14, 8))
    sns.boxplot(data=df_top, x='customer_state', y='delivery_days_actual',
                order=state_order, palette='coolwarm', ax=ax,
                fliersize=2, linewidth=1.5)

    ax.set_xlabel('Customer State')
    ax.set_ylabel('Delivery Days')
    ax.set_title('Delivery Time Distribution by State (Top 10 by Volume)',
                 fontsize=18, fontweight='bold', pad=15)
    ax.axhline(y=df_top['delivery_days_actual'].median(), color='red',
               linestyle='--', alpha=0.7, label=f'Overall Median: {df_top["delivery_days_actual"].median():.0f} days')
    ax.legend()

    fig.tight_layout()
    save_plot(fig, 'delivery_time_by_state.png')


# =====================================================
# PLOT 7: Revenue by State (Bar Chart)
# =====================================================

def plot_revenue_by_state(engine):
    """Horizontal bar: Revenue by top 10 customer states."""
    print("  [7/8] Revenue by State...")

    query = """
    SELECT
        c.customer_state,
        ROUND(SUM(oi.total_value), 2) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders
    FROM fact_orders o
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_state
    ORDER BY revenue DESC
    LIMIT 10
    """
    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots(figsize=(12, 7))
    colors = sns.color_palette('Blues_r', len(df))
    bars = ax.barh(df['customer_state'], df['revenue'], color=colors, edgecolor='white')

    for bar, val in zip(bars, df['revenue']):
        ax.text(bar.get_width() + 10000, bar.get_y() + bar.get_height() / 2,
                f'R${val:,.0f}', va='center', fontweight='bold', fontsize=10)

    ax.set_xlabel('Total Revenue (R$)')
    ax.set_title('Revenue by Customer State (Top 10)', fontsize=18, fontweight='bold', pad=15)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R${x:,.0f}'))

    fig.tight_layout()
    save_plot(fig, 'revenue_by_state.png')


# =====================================================
# PLOT 8: Orders by Day-of-Week & Hour Heatmap
# =====================================================

def plot_order_heatmap(engine):
    """Heatmap: Order count by day of week × hour of day."""
    print("  [8/8] Order Heatmap (Day × Hour)...")

    query = """
    SELECT
        DAYNAME(order_purchase_timestamp) AS day_name,
        DAYOFWEEK(order_purchase_timestamp) AS day_num,
        HOUR(order_purchase_timestamp) AS hour,
        COUNT(*) AS order_count
    FROM fact_orders
    WHERE order_status = 'delivered'
    GROUP BY day_name, day_num, hour
    """
    df = pd.read_sql(query, engine)

    # Pivot for heatmap
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot = df.pivot_table(values='order_count', index='day_name',
                           columns='hour', fill_value=0, aggfunc='sum')
    pivot = pivot.reindex(day_order).fillna(0).astype(int)

    fig, ax = plt.subplots(figsize=(16, 7))
    sns.heatmap(pivot, cmap='YlOrRd', annot=True, fmt='d', linewidths=0.5,
                ax=ax, cbar_kws={'label': 'Order Count'})

    ax.set_xlabel('Hour of Day')
    ax.set_ylabel('Day of Week')
    ax.set_title('Order Volume by Day of Week & Hour',
                 fontsize=18, fontweight='bold', pad=15)

    fig.tight_layout()
    save_plot(fig, 'order_heatmap_day_hour.png')


# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 60)
    print("  E-COMMERCE BI PROJECT — EDA VISUALIZATIONS")
    print("=" * 60)

    engine = get_engine()

    plot_monthly_revenue(engine)
    plot_order_status(engine)
    plot_top_categories(engine)
    plot_payment_types(engine)
    plot_review_scores(engine)
    plot_delivery_by_state(engine)
    plot_revenue_by_state(engine)
    plot_order_heatmap(engine)

    print("\n" + "=" * 60)
    print(f"  ✅ All 8 plots saved to: {PLOTS_DIR}")
    print("=" * 60)


if __name__ == '__main__':
    main()
