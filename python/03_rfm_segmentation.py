"""
E-Commerce BI Project - RFM Customer Segmentation with K-Means
Combines SQL-based RFM scoring with unsupervised clustering

Usage:
    1. Run ETL pipeline first (01_etl_pipeline.py)
    2. Update MYSQL_CONFIG with your credentials
    3. Run: python python/03_rfm_segmentation.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
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
# STEP 1: EXTRACT RFM DATA FROM MYSQL
# =====================================================

def get_rfm_data(engine):
    """Pull customer-level RFM metrics from MySQL."""
    print("\n  [Step 1] Extracting RFM data from MySQL...")

    query = """
    SELECT
        c.customer_unique_id,
        DATEDIFF(
            (SELECT MAX(order_purchase_timestamp) FROM fact_orders),
            MAX(o.order_purchase_timestamp)
        ) AS recency,
        COUNT(DISTINCT o.order_id) AS frequency,
        ROUND(SUM(oi.total_value), 2) AS monetary
    FROM dim_customers c
    JOIN fact_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
    HAVING SUM(oi.total_value) > 0
    """
    df = pd.read_sql(query, engine)
    print(f"    → {df.shape[0]:,} customers extracted")
    print(f"    → Recency: {df['recency'].min()}-{df['recency'].max()} days")
    print(f"    → Frequency: {df['frequency'].min()}-{df['frequency'].max()} orders")
    print(f"    → Monetary: R${df['monetary'].min():.2f} - R${df['monetary'].max():.2f}")
    return df


# =====================================================
# STEP 2: PREPROCESS FOR CLUSTERING
# =====================================================

def preprocess_rfm(df):
    """Log-transform and standardize RFM features."""
    print("\n  [Step 2] Preprocessing RFM features...")

    rfm = df[['recency', 'frequency', 'monetary']].copy()

    # Log-transform frequency and monetary (right-skewed)
    rfm['frequency_log'] = np.log1p(rfm['frequency'])
    rfm['monetary_log'] = np.log1p(rfm['monetary'])
    # Recency is more normally distributed, but log helps
    rfm['recency_log'] = np.log1p(rfm['recency'])

    # Standardize
    scaler = StandardScaler()
    features = ['recency_log', 'frequency_log', 'monetary_log']
    rfm_scaled = pd.DataFrame(
        scaler.fit_transform(rfm[features]),
        columns=features,
        index=rfm.index
    )

    print(f"    → Log-transformed and standardized 3 features")
    return rfm, rfm_scaled, features


# =====================================================
# STEP 3: FIND OPTIMAL K (Elbow + Silhouette)
# =====================================================

def find_optimal_k(rfm_scaled, features, k_range=range(2, 9)):
    """Elbow method + Silhouette scores to find optimal K."""
    print("\n  [Step 3] Finding optimal K...")

    inertias = []
    silhouettes = []
    K_values = list(k_range)

    for k in K_values:
        km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        labels = km.fit_predict(rfm_scaled[features])
        inertias.append(km.inertia_)
        sil = silhouette_score(rfm_scaled[features], labels)
        silhouettes.append(sil)
        print(f"    K={k}: Inertia={km.inertia_:,.0f}, Silhouette={sil:.4f}")

    # Plot Elbow + Silhouette
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Elbow plot
    ax1.plot(K_values, inertias, 'b-o', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Clusters (K)')
    ax1.set_ylabel('Inertia (Within-Cluster Sum of Squares)')
    ax1.set_title('Elbow Method', fontsize=16, fontweight='bold')
    ax1.set_xticks(K_values)
    ax1.grid(True, alpha=0.3)

    # Silhouette plot
    ax2.plot(K_values, silhouettes, 'g-o', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Clusters (K)')
    ax2.set_ylabel('Silhouette Score')
    ax2.set_title('Silhouette Analysis', fontsize=16, fontweight='bold')
    ax2.set_xticks(K_values)
    ax2.grid(True, alpha=0.3)

    # Highlight best K
    best_k = K_values[np.argmax(silhouettes)]
    ax2.axvline(x=best_k, color='red', linestyle='--', alpha=0.7,
                label=f'Best K={best_k} (Score={max(silhouettes):.4f})')
    ax2.legend()

    fig.suptitle('Optimal Cluster Selection', fontsize=18, fontweight='bold', y=1.02)
    fig.tight_layout()
    save_plot(fig, 'rfm_elbow_silhouette.png')

    print(f"\n    → Best K by Silhouette: {best_k}")
    return best_k


# =====================================================
# STEP 4: RUN K-MEANS CLUSTERING
# =====================================================

def run_kmeans(df, rfm, rfm_scaled, features, k):
    """Run K-Means and assign cluster labels."""
    print(f"\n  [Step 4] Running K-Means with K={k}...")

    km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
    df['cluster'] = km.fit_predict(rfm_scaled[features])

    # Assign business-friendly labels based on cluster characteristics
    cluster_summary = df.groupby('cluster').agg({
        'recency': 'mean',
        'frequency': 'mean',
        'monetary': 'mean',
        'customer_unique_id': 'count'
    }).rename(columns={'customer_unique_id': 'count'})

    # Sort by monetary (highest spend = best segment)
    cluster_summary = cluster_summary.sort_values('monetary', ascending=False)

    # Label mapping (rank by monetary value)
    label_map = {}
    labels = ['Champions', 'Loyal Customers', 'Potential Loyalists',
              'At Risk', 'Need Attention', 'Lost', 'Hibernating', 'New']
    for i, cluster_id in enumerate(cluster_summary.index):
        label_map[cluster_id] = labels[min(i, len(labels) - 1)]

    df['segment'] = df['cluster'].map(label_map)

    print("\n    Cluster Summary:")
    print("    " + "-" * 70)
    for cluster_id in cluster_summary.index:
        row = cluster_summary.loc[cluster_id]
        label = label_map[cluster_id]
        print(f"    Cluster {cluster_id} ({label}): "
              f"n={row['count']:,.0f}, "
              f"R={row['recency']:.0f}d, "
              f"F={row['frequency']:.1f}, "
              f"M=R${row['monetary']:.2f}")

    return df, cluster_summary


# =====================================================
# STEP 5: VISUALIZATIONS
# =====================================================

def plot_cluster_scatter(df):
    """3D-style scatter: Recency vs Monetary, sized by Frequency."""
    print("\n  [Step 5a] Cluster Scatter Plot...")

    fig, ax = plt.subplots(figsize=(14, 9))

    for segment in df['segment'].unique():
        mask = df['segment'] == segment
        ax.scatter(
            df.loc[mask, 'recency'],
            df.loc[mask, 'monetary'],
            s=df.loc[mask, 'frequency'] * 30 + 10,
            alpha=0.5,
            label=segment,
            edgecolors='white',
            linewidth=0.5
        )

    ax.set_xlabel('Recency (Days Since Last Purchase)')
    ax.set_ylabel('Monetary Value (R$)')
    ax.set_title('Customer Segments: Recency vs Monetary (Size = Frequency)',
                 fontsize=16, fontweight='bold', pad=15)
    ax.legend(title='Segment', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R${x:,.0f}'))

    fig.tight_layout()
    save_plot(fig, 'rfm_cluster_scatter.png')


def plot_snake_plot(df):
    """Snake plot: Normalized RFM means per cluster."""
    print("  [Step 5b] Snake Plot...")

    # Normalize each feature to 0-1 for comparison
    rfm_norm = df.copy()
    for col in ['recency', 'frequency', 'monetary']:
        rfm_norm[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

    # For recency, invert (lower = better)
    rfm_norm['recency'] = 1 - rfm_norm['recency']

    cluster_means = rfm_norm.groupby('segment')[['recency', 'frequency', 'monetary']].mean()

    fig, ax = plt.subplots(figsize=(10, 7))
    for segment in cluster_means.index:
        ax.plot(['Recency\n(inverted)', 'Frequency', 'Monetary'],
                cluster_means.loc[segment],
                marker='o', linewidth=2.5, markersize=10, label=segment)

    ax.set_ylabel('Normalized Score (0-1)')
    ax.set_title('Customer Segment Profiles (Snake Plot)',
                 fontsize=16, fontweight='bold', pad=15)
    ax.legend(title='Segment', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.set_ylim(-0.05, 1.05)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    save_plot(fig, 'rfm_snake_plot.png')


def plot_revenue_share(df):
    """Bar + pie: Revenue share by segment."""
    print("  [Step 5c] Revenue Share by Segment...")

    seg_revenue = df.groupby('segment').agg({
        'monetary': 'sum',
        'customer_unique_id': 'count'
    }).rename(columns={'customer_unique_id': 'customer_count'})
    seg_revenue['revenue_pct'] = seg_revenue['monetary'] / seg_revenue['monetary'].sum() * 100
    seg_revenue['customer_pct'] = seg_revenue['customer_count'] / seg_revenue['customer_count'].sum() * 100
    seg_revenue = seg_revenue.sort_values('monetary', ascending=False)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Bar chart
    colors = sns.color_palette('viridis', len(seg_revenue))
    bars = ax1.barh(seg_revenue.index, seg_revenue['revenue_pct'],
                     color=colors, edgecolor='white')
    for bar, pct in zip(bars, seg_revenue['revenue_pct']):
        ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{pct:.1f}%', va='center', fontweight='bold')
    ax1.set_xlabel('Revenue Share (%)')
    ax1.set_title('Revenue Contribution by Segment', fontsize=14, fontweight='bold')
    ax1.invert_yaxis()

    # Pie chart (customer count)
    ax2.pie(seg_revenue['customer_count'], labels=seg_revenue.index,
            autopct='%1.1f%%', colors=colors, startangle=90,
            wedgeprops=dict(edgecolor='white', linewidth=1.5))
    ax2.set_title('Customer Distribution by Segment', fontsize=14, fontweight='bold')

    fig.suptitle('RFM Segment Analysis', fontsize=18, fontweight='bold', y=1.02)
    fig.tight_layout()
    save_plot(fig, 'rfm_segment_revenue_share.png')

    # Print key insight
    if len(seg_revenue) > 0:
        top_seg = seg_revenue.index[0]
        top_rev = seg_revenue.iloc[0]['revenue_pct']
        top_cust = seg_revenue.iloc[0]['customer_pct']
        print(f"\n    💡 KEY INSIGHT: '{top_seg}' segment is {top_cust:.1f}% of customers "
              f"but drives {top_rev:.1f}% of revenue")


def plot_cluster_boxplots(df):
    """Box plots of R, F, M per cluster."""
    print("  [Step 5d] Cluster Box Plots...")

    fig, axes = plt.subplots(1, 3, figsize=(18, 7))

    for ax, col, label, fmt in zip(
        axes,
        ['recency', 'frequency', 'monetary'],
        ['Recency (Days)', 'Frequency (Orders)', 'Monetary (R$)'],
        ['{:.0f}', '{:.0f}', 'R${:,.0f}']
    ):
        sns.boxplot(data=df, x='segment', y=col, ax=ax,
                    palette='viridis', fliersize=2)
        ax.set_xlabel('')
        ax.set_ylabel(label)
        ax.set_title(label, fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', rotation=30)

    fig.suptitle('RFM Distribution by Customer Segment',
                 fontsize=18, fontweight='bold', y=1.02)
    fig.tight_layout()
    save_plot(fig, 'rfm_cluster_boxplots.png')


# =====================================================
# STEP 6: EXPORT CLUSTERS BACK TO MYSQL
# =====================================================

def export_clusters(engine, df):
    """Save cluster assignments to MySQL for Power BI."""
    print("\n  [Step 6] Exporting cluster assignments to MySQL...")

    export_df = df[['customer_unique_id', 'recency', 'frequency',
                     'monetary', 'cluster', 'segment']].copy()

    export_df.to_sql('rfm_clusters', engine, if_exists='replace',
                      index=False, chunksize=5000)
    print(f"    ✓ Exported {len(export_df):,} rows to 'rfm_clusters' table")


# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 60)
    print("  E-COMMERCE BI PROJECT — RFM SEGMENTATION")
    print("=" * 60)

    engine = get_engine()

    # Step 1: Get RFM data
    df = get_rfm_data(engine)

    # Step 2: Preprocess
    rfm, rfm_scaled, features = preprocess_rfm(df)

    # Step 3: Find optimal K
    best_k = find_optimal_k(rfm_scaled, features)

    # Step 4: Run K-Means
    df, cluster_summary = run_kmeans(df, rfm, rfm_scaled, features, best_k)

    # Step 5: Visualizations
    print("\n  [Step 5] Generating visualizations...")
    plot_cluster_scatter(df)
    plot_snake_plot(df)
    plot_revenue_share(df)
    plot_cluster_boxplots(df)

    # Step 6: Export to MySQL
    export_clusters(engine, df)

    print("\n" + "=" * 60)
    print("  ✅ RFM SEGMENTATION COMPLETE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
