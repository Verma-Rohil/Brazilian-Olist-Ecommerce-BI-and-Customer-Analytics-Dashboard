"""
E-Commerce BI Project - Correlation & Hypothesis Testing
Statistical analysis of delivery performance vs customer satisfaction

Usage:
    1. Run ETL pipeline first (01_etl_pipeline.py)
    2. Update MYSQL_CONFIG with your credentials
    3. Run: python python/04_correlation_analysis.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats
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

SIGNIFICANCE_LEVEL = 0.05


def get_engine():
    return create_engine(ENGINE_URL, echo=False)


def save_plot(fig, filename):
    path = os.path.join(PLOTS_DIR, filename)
    fig.savefig(path)
    plt.close(fig)
    print(f"  ✓ Saved: {filename}")


def significance_label(p_value, alpha=0.05):
    """Return significance label."""
    if p_value < 0.001:
        return "*** (p < 0.001)"
    elif p_value < 0.01:
        return "** (p < 0.01)"
    elif p_value < alpha:
        return "* (p < 0.05)"
    else:
        return "ns (not significant)"


# =====================================================
# STEP 1: EXTRACT DATA FOR ANALYSIS
# =====================================================

def get_analysis_data(engine):
    """Pull combined order-delivery-review data."""
    print("\n  [Step 1] Extracting analysis data...")

    query = """
    SELECT
        o.order_id,
        o.delivery_days_actual,
        o.delivery_days_estimated,
        o.delivery_delta_days,
        o.is_late_delivery,
        r.review_score,
        oi.price,
        oi.freight_value,
        oi.total_value,
        p.product_weight_g,
        p.product_category_english AS category,
        c.customer_state
    FROM fact_orders o
    JOIN fact_reviews r ON o.order_id = r.order_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND o.delivery_days_actual IS NOT NULL
      AND o.delivery_days_actual BETWEEN 0 AND 60
    """
    df = pd.read_sql(query, engine)
    print(f"    → {df.shape[0]:,} rows extracted")
    return df


# =====================================================
# STEP 2: CORRELATION MATRIX
# =====================================================

def analyze_correlations(df):
    """Compute and visualize correlation matrix."""
    print("\n  [Step 2] Computing correlation matrix...")

    numeric_cols = ['delivery_days_actual', 'delivery_delta_days',
                    'freight_value', 'price', 'review_score', 'product_weight_g']
    corr_df = df[numeric_cols].dropna()

    # Pearson correlation
    corr_pearson = corr_df.corr(method='pearson')
    # Spearman correlation (rank-based, handles non-linear)
    corr_spearman = corr_df.corr(method='spearman')

    # Plot Pearson correlation heatmap
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))

    # Pearson
    mask = np.triu(np.ones_like(corr_pearson, dtype=bool))
    sns.heatmap(corr_pearson, mask=mask, annot=True, fmt='.3f', cmap='RdBu_r',
                center=0, vmin=-1, vmax=1, linewidths=0.5, ax=ax1,
                square=True, cbar_kws={'shrink': 0.8})
    ax1.set_title('Pearson Correlation', fontsize=14, fontweight='bold')

    # Spearman
    sns.heatmap(corr_spearman, mask=mask, annot=True, fmt='.3f', cmap='RdBu_r',
                center=0, vmin=-1, vmax=1, linewidths=0.5, ax=ax2,
                square=True, cbar_kws={'shrink': 0.8})
    ax2.set_title('Spearman Correlation', fontsize=14, fontweight='bold')

    fig.suptitle('Correlation Analysis: Delivery, Price, Weight & Satisfaction',
                 fontsize=16, fontweight='bold', y=1.02)
    fig.tight_layout()
    save_plot(fig, 'correlation_heatmap.png')

    # Key correlations
    print("\n    Key Pearson Correlations:")
    print(f"    • Delivery Days vs Review Score: {corr_pearson.loc['delivery_days_actual', 'review_score']:.4f}")
    print(f"    • Freight vs Product Weight:     {corr_pearson.loc['freight_value', 'product_weight_g']:.4f}")
    print(f"    • Price vs Review Score:         {corr_pearson.loc['price', 'review_score']:.4f}")

    return corr_pearson, corr_spearman


# =====================================================
# STEP 3: HYPOTHESIS TEST 1 — Late Delivery vs Review Score
# =====================================================

def test_delivery_vs_review(df):
    """
    H₀: Late deliveries have no effect on review scores
    H₁: Late deliveries lead to lower review scores
    Method: Welch's t-test (unequal variances)
    """
    print("\n  [Step 3] Hypothesis Test 1: Late Delivery → Lower Review?")
    print("  " + "-" * 60)

    on_time = df[df['is_late_delivery'] == 0]['review_score']
    late = df[df['is_late_delivery'] == 1]['review_score']

    print(f"    On-time deliveries: n={len(on_time):,}, mean={on_time.mean():.3f}, std={on_time.std():.3f}")
    print(f"    Late deliveries:    n={len(late):,}, mean={late.mean():.3f}, std={late.std():.3f}")

    # Welch's t-test (independent, two-sample, unequal variance)
    t_stat, p_value = stats.ttest_ind(on_time, late, equal_var=False)
    # Effect size (Cohen's d)
    pooled_std = np.sqrt((on_time.std()**2 + late.std()**2) / 2)
    cohens_d = (on_time.mean() - late.mean()) / pooled_std

    print(f"\n    Welch's t-test:")
    print(f"    t-statistic: {t_stat:.4f}")
    print(f"    p-value:     {p_value:.2e}  →  {significance_label(p_value)}")
    print(f"    Cohen's d:   {cohens_d:.4f} ({'Small' if abs(cohens_d) < 0.5 else 'Medium' if abs(cohens_d) < 0.8 else 'Large'} effect)")
    print(f"\n    📊 Result: {'REJECT' if p_value < SIGNIFICANCE_LEVEL else 'FAIL TO REJECT'} H₀")
    print(f"    → Late deliveries {'DO' if p_value < SIGNIFICANCE_LEVEL else 'do NOT'} significantly affect review scores.")

    # Visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Box plot
    groups = ['On Time', 'Late']
    data = [on_time, late]
    bp = ax1.boxplot(data, labels=groups, patch_artist=True, widths=0.5)
    bp['boxes'][0].set_facecolor('#44AF69')
    bp['boxes'][1].set_facecolor('#D64045')
    ax1.set_ylabel('Review Score')
    ax1.set_title('Review Score: On-Time vs Late Delivery',
                  fontsize=14, fontweight='bold')
    ax1.text(0.5, -0.12, f'p = {p_value:.2e} | Cohen\'s d = {cohens_d:.3f}',
             transform=ax1.transAxes, ha='center', fontsize=11,
             style='italic', color='gray')

    # Distribution overlay
    ax2.hist(on_time, bins=5, alpha=0.6, color='#44AF69', label=f'On Time (μ={on_time.mean():.2f})',
             density=True, edgecolor='white')
    ax2.hist(late, bins=5, alpha=0.6, color='#D64045', label=f'Late (μ={late.mean():.2f})',
             density=True, edgecolor='white')
    ax2.set_xlabel('Review Score')
    ax2.set_ylabel('Density')
    ax2.set_title('Score Distribution Comparison', fontsize=14, fontweight='bold')
    ax2.legend()

    fig.suptitle('Hypothesis Test: Does Late Delivery Affect Customer Satisfaction?',
                 fontsize=16, fontweight='bold', y=1.02)
    fig.tight_layout()
    save_plot(fig, 'late_vs_ontime_review_boxplot.png')

    return {'test': "Welch's t-test", 't_stat': t_stat, 'p_value': p_value, 'cohens_d': cohens_d}


# =====================================================
# STEP 4: HYPOTHESIS TEST 2 — Freight vs Review Score
# =====================================================

def test_freight_vs_review(df):
    """
    H₀: Freight value has no correlation with review score
    H₁: Freight value is correlated with review score
    Method: Spearman rank correlation
    """
    print("\n  [Step 4] Hypothesis Test 2: Freight → Review Score?")
    print("  " + "-" * 60)

    clean = df[['freight_value', 'review_score']].dropna()

    corr, p_value = stats.spearmanr(clean['freight_value'], clean['review_score'])

    print(f"    Spearman correlation: r = {corr:.4f}")
    print(f"    p-value:              {p_value:.2e}  →  {significance_label(p_value)}")
    print(f"\n    📊 Result: {'REJECT' if p_value < SIGNIFICANCE_LEVEL else 'FAIL TO REJECT'} H₀")

    return {'test': 'Spearman', 'correlation': corr, 'p_value': p_value}


# =====================================================
# STEP 5: HYPOTHESIS TEST 3 — Product Weight vs Delivery Time
# =====================================================

def test_weight_vs_delivery(df):
    """
    H₀: Product weight has no effect on delivery time
    H₁: Heavier products take longer to deliver
    Method: Pearson correlation
    """
    print("\n  [Step 5] Hypothesis Test 3: Product Weight → Delivery Time?")
    print("  " + "-" * 60)

    clean = df[['product_weight_g', 'delivery_days_actual']].dropna()

    corr, p_value = stats.pearsonr(clean['product_weight_g'], clean['delivery_days_actual'])

    print(f"    Pearson correlation:  r = {corr:.4f}")
    print(f"    p-value:              {p_value:.2e}  →  {significance_label(p_value)}")
    print(f"\n    📊 Result: {'REJECT' if p_value < SIGNIFICANCE_LEVEL else 'FAIL TO REJECT'} H₀")

    # Scatter plot
    fig, ax = plt.subplots(figsize=(10, 7))
    # Sample for visibility (too many points otherwise)
    sample = clean.sample(min(5000, len(clean)), random_state=42)
    ax.scatter(sample['product_weight_g'] / 1000, sample['delivery_days_actual'],
               alpha=0.15, s=10, color='#2E86AB')

    # Regression line
    z = np.polyfit(clean['product_weight_g'] / 1000, clean['delivery_days_actual'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(0, clean['product_weight_g'].max() / 1000, 100)
    ax.plot(x_line, p(x_line), 'r-', linewidth=2,
            label=f'r = {corr:.4f}, p = {p_value:.2e}')

    ax.set_xlabel('Product Weight (kg)')
    ax.set_ylabel('Delivery Days')
    ax.set_title('Product Weight vs Delivery Time',
                 fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=12)

    fig.tight_layout()
    save_plot(fig, 'weight_vs_delivery_scatter.png')

    return {'test': 'Pearson', 'correlation': corr, 'p_value': p_value}


# =====================================================
# STEP 6: DELIVERY BUCKET vs REVIEW — Detailed Analysis
# =====================================================

def delivery_bucket_analysis(df):
    """Visualize review scores across delivery timing buckets."""
    print("\n  [Step 6] Delivery Bucket Analysis...")

    df_copy = df.copy()
    df_copy['delivery_bucket'] = pd.cut(
        df_copy['delivery_delta_days'],
        bins=[-999, -5, -1, 0, 9, 999],
        labels=['Very Late\n(5+ days)', 'Slightly Late\n(1-5 days)',
                'On Time', 'Early\n(1-9 days)', 'Very Early\n(10+ days)']
    )

    fig, ax = plt.subplots(figsize=(12, 7))

    bucket_order = ['Very Late\n(5+ days)', 'Slightly Late\n(1-5 days)',
                    'On Time', 'Early\n(1-9 days)', 'Very Early\n(10+ days)']
    colors = ['#D64045', '#E8630A', '#F8C630', '#96C93D', '#44AF69']

    sns.boxplot(data=df_copy, x='delivery_bucket', y='review_score',
                order=bucket_order, palette=colors, ax=ax,
                linewidth=1.5, fliersize=2)

    # Add mean markers
    means = df_copy.groupby('delivery_bucket')['review_score'].mean()
    for i, bucket in enumerate(bucket_order):
        if bucket in means.index:
            ax.scatter(i, means[bucket], color='black', s=100, zorder=5,
                      marker='D', edgecolors='white', linewidth=1.5)
            ax.annotate(f'μ={means[bucket]:.2f}',
                       xy=(i, means[bucket]), xytext=(10, 10),
                       textcoords='offset points', fontsize=10, fontweight='bold')

    ax.set_xlabel('Delivery Timing')
    ax.set_ylabel('Review Score (1-5)')
    ax.set_title('Customer Satisfaction by Delivery Timing',
                 fontsize=16, fontweight='bold', pad=15)

    fig.tight_layout()
    save_plot(fig, 'delivery_bucket_review.png')


# =====================================================
# STEP 7: SUMMARY REPORT
# =====================================================

def print_summary(results):
    """Print summary of all hypothesis tests."""
    print("\n" + "=" * 60)
    print("  STATISTICAL ANALYSIS SUMMARY")
    print("=" * 60)

    print(f"""
    Test 1: Late Delivery vs Review Score
    ├── Method:    Welch's t-test
    ├── p-value:   {results[0]['p_value']:.2e}
    ├── Effect:    Cohen's d = {results[0]['cohens_d']:.4f}
    └── Decision:  {'REJECT' if results[0]['p_value'] < 0.05 else 'FAIL TO REJECT'} H₀

    Test 2: Freight Value vs Review Score
    ├── Method:    Spearman Correlation
    ├── r:         {results[1]['correlation']:.4f}
    ├── p-value:   {results[1]['p_value']:.2e}
    └── Decision:  {'REJECT' if results[1]['p_value'] < 0.05 else 'FAIL TO REJECT'} H₀

    Test 3: Product Weight vs Delivery Time
    ├── Method:    Pearson Correlation
    ├── r:         {results[2]['correlation']:.4f}
    ├── p-value:   {results[2]['p_value']:.2e}
    └── Decision:  {'REJECT' if results[2]['p_value'] < 0.05 else 'FAIL TO REJECT'} H₀
    """)


# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 60)
    print("  E-COMMERCE BI PROJECT — CORRELATION & HYPOTHESIS TESTING")
    print("=" * 60)

    engine = get_engine()

    # Step 1: Get data
    df = get_analysis_data(engine)

    # Step 2: Correlation matrix
    corr_p, corr_s = analyze_correlations(df)

    # Steps 3-5: Hypothesis tests
    results = []
    results.append(test_delivery_vs_review(df))
    results.append(test_freight_vs_review(df))
    results.append(test_weight_vs_delivery(df))

    # Step 6: Delivery bucket analysis
    delivery_bucket_analysis(df)

    # Step 7: Summary
    print_summary(results)

    print("=" * 60)
    print("  ✅ CORRELATION ANALYSIS COMPLETE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
