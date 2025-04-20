from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import numpy as np
import os

# Define file paths for intermediate data
EXTRACT_FILE = '/tmp/orders_extracted.csv'
CUSTOMERS_FILE = '/tmp/dim_customer.csv'
PRODUCTS_FILE = '/tmp/dim_product.csv'
DATES_FILE = '/tmp/dim_date.csv'
FACT_SALES_FILE = '/tmp/fact_sales.csv'

def create_transactional_data():
    # Generate realistic transactional data
    np.random.seed(42)
    n_orders = 10000
    customers = [f"Customer_{i}" for i in range(1, 501)]  # 500 unique customers
    products = [f"Product_{i}" for i in range(1, 101)]    # 100 unique products
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]  # 1 year of dates

    data = {
        'order_id': range(1, n_orders + 1),
        'customer_name': np.random.choice(customers, n_orders),
        'product': np.random.choice(products, n_orders),
        'amount': np.random.uniform(50, 5000, n_orders).round(2),
        'order_date': [d.strftime('%Y-%m-%d') for d in np.random.choice(dates, n_orders)]
    }
    df = pd.DataFrame(data)
    
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    df.to_sql('orders', engine, if_exists='replace', index=False)

def extract_data():
    # Extract data from transactional table
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    orders = pd.read_sql("SELECT * FROM orders", engine)
    orders.to_csv(EXTRACT_FILE, index=False)

def transform_data():
    # Read extracted data
    orders = pd.read_csv(EXTRACT_FILE)
    
    # Dimension: Customers
    customers = orders[['customer_name']].drop_duplicates().reset_index(drop=True)
    customers['customer_id'] = range(1, len(customers) + 1)
    customers.to_csv(CUSTOMERS_FILE, index=False)
    
    # Dimension: Products
    products = orders[['product']].drop_duplicates().reset_index(drop=True)
    products['product_id'] = range(1, len(products) + 1)
    products.to_csv(PRODUCTS_FILE, index=False)
    
    # Dimension: Dates
    dates = orders[['order_date']].drop_duplicates().reset_index(drop=True)
    dates['date_id'] = range(1, len(dates) + 1)
    dates['year'] = pd.to_datetime(dates['order_date']).dt.year
    dates['month'] = pd.to_datetime(dates['order_date']).dt.month
    dates['day'] = pd.to_datetime(dates['order_date']).dt.day
    dates.to_csv(DATES_FILE, index=False)
    
    # Fact: Sales
    fact_sales = orders.merge(customers, on='customer_name')
    fact_sales = fact_sales.merge(products, on='product')
    fact_sales = fact_sales.merge(dates, on='order_date')
    fact_sales = fact_sales[['order_id', 'customer_id', 'product_id', 'date_id', 'amount']]
    fact_sales.to_csv(FACT_SALES_FILE, index=False)

def load_data():
    # Create dwh schema
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
    
    # Load dimension tables
    customers = pd.read_csv(CUSTOMERS_FILE)
    customers.to_sql('dim_customer', engine, schema='dwh', if_exists='replace', index=False)
    
    products = pd.read_csv(PRODUCTS_FILE)
    products.to_sql('dim_product', engine, schema='dwh', if_exists='replace', index=False)
    
    dates = pd.read_csv(DATES_FILE)
    dates.to_sql('dim_date', engine, schema='dwh', if_exists='replace', index=False)
    
    # Load fact table
    fact_sales = pd.read_csv(FACT_SALES_FILE)
    fact_sales.to_sql('fact_sales', engine, schema='dwh', if_exists='replace', index=False)
    
    # Clean up temporary files
    for file in [EXTRACT_FILE, CUSTOMERS_FILE, PRODUCTS_FILE, DATES_FILE, FACT_SALES_FILE]:
        if os.path.exists(file):
            os.remove(file)

with DAG('star_schema_dag', start_date=datetime(2025, 4, 18), schedule_interval='@daily', catchup=False) as dag:
    create_data_task = PythonOperator(
        task_id='create_transactional_data',
        python_callable=create_transactional_data
    )
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    
    create_data_task >> extract_task >> transform_task >> load_task