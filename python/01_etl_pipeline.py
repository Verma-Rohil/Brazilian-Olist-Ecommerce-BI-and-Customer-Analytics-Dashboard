"""
E-Commerce BI Project - ETL Pipeline
Loads 8 Olist CSV files → cleans → engineers features → loads into MySQL

Usage:
    1. Place all Olist CSV files in data/raw/
    2. Update MYSQL_CONFIG with your credentials
    3. Run: python python/01_etl_pipeline.py
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
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

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')

# Connection string
ENGINE_URL = (
    f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
    f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
)

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def get_engine():
    """Create SQLAlchemy engine for MySQL connection."""
    return create_engine(ENGINE_URL, echo=False)


def load_csv(filename):
    """Load a CSV from the data/raw directory."""
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"  [WARNING] File not found: {filepath}")
        return None
    df = pd.read_csv(filepath)
    print(f"  Loaded {filename}: {df.shape[0]:,} rows x {df.shape[1]} cols")
    return df


def safe_to_datetime(series):
    """Convert to datetime, coercing errors to NaT."""
    return pd.to_datetime(series, errors='coerce')


# =====================================================
# STEP 1: LOAD RAW CSVs
# =====================================================

def load_all_raw_data():
    """Load all 8 Olist CSV files into DataFrames."""
    print("\n" + "=" * 60)
    print("STEP 1: Loading raw CSV files")
    print("=" * 60)

    data = {}
    data['customers']    = load_csv('olist_customers_dataset.csv')
    data['orders']       = load_csv('olist_orders_dataset.csv')
    data['order_items']  = load_csv('olist_order_items_dataset.csv')
    data['payments']     = load_csv('olist_order_payments_dataset.csv')
    data['reviews']      = load_csv('olist_order_reviews_dataset.csv')
    data['products']     = load_csv('olist_products_dataset.csv')
    data['sellers']      = load_csv('olist_sellers_dataset.csv')
    data['geolocation']  = load_csv('olist_geolocation_dataset.csv')
    data['category_translation'] = load_csv('product_category_name_translation.csv')

    # Check for missing files
    missing = [k for k, v in data.items() if v is None]
    if missing:
        print(f"\n  [ERROR] Missing files: {missing}")
        print("  Please download the Olist dataset from Kaggle and place CSVs in data/raw/")
        return None

    return data


# =====================================================
# STEP 2: CLEAN & TRANSFORM
# =====================================================

def clean_customers(df):
    """Clean customers dataset."""
    print("\n  Cleaning customers...")
    df = df.drop_duplicates(subset='customer_id')
    df = df.rename(columns={'customer_zip_code_prefix': 'customer_zip_prefix'})
    # Trim whitespace
    for col in ['customer_city', 'customer_state']:
        df[col] = df[col].str.strip().str.title()
    print(f"    → {df.shape[0]:,} unique customers")
    return df


def clean_products(df, translation_df):
    """Clean products, merge English category names."""
    print("\n  Cleaning products...")
    df = df.drop_duplicates(subset='product_id')

    # Merge English translations
    df = df.merge(
        translation_df,
        on='product_category_name',
        how='left'
    )

    # Rename columns
    df = df.rename(columns={
        'product_category_name': 'product_category',
        'product_category_name_english': 'product_category_english',
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    })

    # Fill NaN categories
    df['product_category_english'] = df['product_category_english'].fillna('other')

    print(f"    → {df.shape[0]:,} unique products, {df['product_category_english'].nunique()} categories")
    return df


def clean_sellers(df):
    """Clean sellers dataset."""
    print("\n  Cleaning sellers...")
    df = df.drop_duplicates(subset='seller_id')
    df = df.rename(columns={'seller_zip_code_prefix': 'seller_zip_prefix'})
    for col in ['seller_city', 'seller_state']:
        df[col] = df[col].str.strip().str.title()
    print(f"    → {df.shape[0]:,} unique sellers")
    return df


def clean_geolocation(df):
    """
    Clean geolocation — heavy dedup. 
    Aggregate to unique zip prefixes (take median lat/lng).
    """
    print("\n  Cleaning geolocation...")
    print(f"    Raw rows: {df.shape[0]:,}")
    df = df.rename(columns={'geolocation_zip_code_prefix': 'geolocation_zip_prefix'})

    # Aggregate to unique zip prefixes
    geo_agg = df.groupby('geolocation_zip_prefix').agg({
        'geolocation_lat': 'median',
        'geolocation_lng': 'median',
        'geolocation_city': 'first',
        'geolocation_state': 'first'
    }).reset_index()

    geo_agg['geolocation_city'] = geo_agg['geolocation_city'].str.strip().str.title()
    geo_agg['geolocation_state'] = geo_agg['geolocation_state'].str.strip().str.upper()

    print(f"    → {geo_agg.shape[0]:,} unique zip prefixes")
    return geo_agg


def clean_orders(df):
    """Clean orders, engineer delivery features."""
    print("\n  Cleaning orders...")
    df = df.drop_duplicates(subset='order_id')

    # Convert datetime columns
    datetime_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in datetime_cols:
        df[col] = safe_to_datetime(df[col])

    # ----- Feature Engineering -----

    # Delivery days: actual vs estimated
    df['delivery_days_actual'] = (
        df['order_delivered_customer_date'] - df['order_purchase_timestamp']
    ).dt.days

    df['delivery_days_estimated'] = (
        df['order_estimated_delivery_date'] - df['order_purchase_timestamp']
    ).dt.days

    # Delta: positive = early, negative = late
    df['delivery_delta_days'] = (
        df['order_estimated_delivery_date'] - df['order_delivered_customer_date']
    ).dt.days

    # Late delivery flag
    df['is_late_delivery'] = (df['delivery_delta_days'] < 0).astype(int)
    # Set to 0 for non-delivered orders
    df.loc[df['order_delivered_customer_date'].isna(), 'is_late_delivery'] = 0

    # Order month (for cohort / time-series analysis)
    df['order_month'] = df['order_purchase_timestamp'].dt.strftime('%Y-%m')

    print(f"    → {df.shape[0]:,} orders")
    print(f"    → Delivered: {(df['order_status'] == 'delivered').sum():,}")
    print(f"    → Late deliveries: {df['is_late_delivery'].sum():,}")
    return df


def clean_order_items(df):
    """Clean order items, calculate total_value."""
    print("\n  Cleaning order items...")
    # Derived: total value = price + freight
    df['total_value'] = df['price'] + df['freight_value']

    # Convert shipping limit date
    df['shipping_limit_date'] = safe_to_datetime(df['shipping_limit_date'])

    # Remove items with price <= 0
    n_before = len(df)
    df = df[df['price'] > 0]
    removed = n_before - len(df)
    if removed > 0:
        print(f"    → Removed {removed} items with price <= 0")

    print(f"    → {df.shape[0]:,} order items, avg total_value: R${df['total_value'].mean():.2f}")
    return df


def clean_payments(df):
    """Clean payments dataset."""
    print("\n  Cleaning payments...")
    # Remove zero-value payments
    df = df[df['payment_value'] > 0]
    print(f"    → {df.shape[0]:,} payments, {df['payment_type'].nunique()} types")
    return df


def clean_reviews(df):
    """Clean reviews dataset."""
    print("\n  Cleaning reviews...")
    df = df.drop_duplicates(subset='review_id')

    # Convert datetime columns
    df['review_creation_date'] = safe_to_datetime(df['review_creation_date'])
    df['review_answer_timestamp'] = safe_to_datetime(df['review_answer_timestamp'])

    # Fill NaN comment fields
    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')

    # Validate review_score in [1, 5]
    df = df[df['review_score'].between(1, 5)]

    print(f"    → {df.shape[0]:,} reviews, avg score: {df['review_score'].mean():.2f}")
    return df


# =====================================================
# STEP 3: LOAD INTO MYSQL
# =====================================================

def load_to_mysql(engine, data):
    """Load cleaned DataFrames into MySQL tables."""
    print("\n" + "=" * 60)
    print("STEP 3: Loading data into MySQL")
    print("=" * 60)

    # --- Dimension tables first ---

    print("\n  Loading dim_customers...")
    data['customers'][['customer_id', 'customer_unique_id', 'customer_city',
                        'customer_state', 'customer_zip_prefix']]\
        .to_sql('dim_customers', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ dim_customers loaded")

    print("\n  Loading dim_products...")
    product_cols = ['product_id', 'product_category', 'product_category_english',
                    'product_weight_g', 'product_length_cm', 'product_height_cm',
                    'product_width_cm', 'product_photos_qty',
                    'product_name_length', 'product_description_length']
    data['products'][product_cols]\
        .to_sql('dim_products', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ dim_products loaded")

    print("\n  Loading dim_sellers...")
    data['sellers'][['seller_id', 'seller_city', 'seller_state', 'seller_zip_prefix']]\
        .to_sql('dim_sellers', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ dim_sellers loaded")

    print("\n  Loading dim_geolocation...")
    data['geolocation'].to_sql('dim_geolocation', engine, if_exists='append',
                                index=False, chunksize=5000)
    print("    ✓ dim_geolocation loaded")

    # --- Fact tables ---

    print("\n  Loading fact_orders...")
    order_cols = ['order_id', 'customer_id', 'order_status',
                  'order_purchase_timestamp', 'order_approved_at',
                  'order_delivered_carrier_date', 'order_delivered_customer_date',
                  'order_estimated_delivery_date', 'delivery_days_actual',
                  'delivery_days_estimated', 'delivery_delta_days',
                  'is_late_delivery', 'order_month']
    data['orders'][order_cols]\
        .to_sql('fact_orders', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ fact_orders loaded")

    print("\n  Loading fact_order_items...")
    item_cols = ['order_id', 'order_item_id', 'product_id', 'seller_id',
                 'shipping_limit_date', 'price', 'freight_value', 'total_value']
    data['order_items'][item_cols]\
        .to_sql('fact_order_items', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ fact_order_items loaded")

    print("\n  Loading fact_payments...")
    pay_cols = ['order_id', 'payment_sequential', 'payment_type',
                'payment_installments', 'payment_value']
    data['payments'][pay_cols]\
        .to_sql('fact_payments', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ fact_payments loaded")

    print("\n  Loading fact_reviews...")
    review_cols = ['review_id', 'order_id', 'review_score',
                   'review_comment_title', 'review_comment_message',
                   'review_creation_date', 'review_answer_timestamp']
    data['reviews'][review_cols]\
        .to_sql('fact_reviews', engine, if_exists='append', index=False, chunksize=5000)
    print("    ✓ fact_reviews loaded")


# =====================================================
# STEP 4: VERIFICATION
# =====================================================

def verify_load(engine, data):
    """Verify row counts in MySQL match source data."""
    print("\n" + "=" * 60)
    print("STEP 4: Verification")
    print("=" * 60)

    tables = {
        'dim_customers': len(data['customers']),
        'dim_products': len(data['products']),
        'dim_sellers': len(data['sellers']),
        'dim_geolocation': len(data['geolocation']),
        'fact_orders': len(data['orders']),
        'fact_order_items': len(data['order_items']),
        'fact_payments': len(data['payments']),
        'fact_reviews': len(data['reviews'])
    }

    with engine.connect() as conn:
        all_good = True
        for table, expected in tables.items():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            actual = result.scalar()
            status = "✓" if actual == expected else "✗"
            if actual != expected:
                all_good = False
            print(f"  {status} {table}: {actual:,} rows (expected: {expected:,})")

    if all_good:
        print("\n  ✅ All tables loaded successfully!")
    else:
        print("\n  ⚠️  Some row counts don't match — review the cleaning steps.")


# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 60)
    print("  E-COMMERCE BI PROJECT — ETL PIPELINE")
    print("=" * 60)

    # Step 1: Load raw CSVs
    raw_data = load_all_raw_data()
    if raw_data is None:
        return

    # Step 2: Clean & transform
    print("\n" + "=" * 60)
    print("STEP 2: Cleaning & transforming data")
    print("=" * 60)

    raw_data['customers']   = clean_customers(raw_data['customers'])
    raw_data['products']    = clean_products(raw_data['products'], raw_data['category_translation'])
    raw_data['sellers']     = clean_sellers(raw_data['sellers'])
    raw_data['geolocation'] = clean_geolocation(raw_data['geolocation'])
    raw_data['orders']      = clean_orders(raw_data['orders'])
    raw_data['order_items'] = clean_order_items(raw_data['order_items'])
    raw_data['payments']    = clean_payments(raw_data['payments'])
    raw_data['reviews']     = clean_reviews(raw_data['reviews'])

    # Step 3: Load into MySQL
    engine = get_engine()
    # Drop and recreate tables by running schema first
    print("\n  Running schema script to create/reset tables...")
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                'sql', '01_schema_design.sql')
    print(f"  ℹ️  Make sure to run {schema_path} in MySQL Workbench first!")
    print("  (This script loads data assuming tables already exist.)\n")

    load_to_mysql(engine, raw_data)

    # Step 4: Verify
    verify_load(engine, raw_data)

    print("\n" + "=" * 60)
    print("  ETL PIPELINE COMPLETE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
