#!/usr/bin/env python
"""
Script to generate additional test data for sales database
"""

import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import argparse

# Initialize Faker
fake = Faker()

# Connection parameters - adjust these to match your setup
DB_PARAMS = {
    'dbname': 'mydatabase',  # As defined in your docker-compose
    'user': 'postgres',      # As defined in your docker-compose
    'password': 'postgres',  # As defined in your docker-compose
    'host': 'localhost',     # Connect from host machine
    'port': '5432'           # Mapped port from your docker-compose
}

def connect_to_db():
    """Establish connection to the database"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def generate_customers(conn, num_customers=10):
    """Generate new customers"""
    customers = []
    
    for _ in range(num_customers):
        customer = (
            fake.name(),
            fake.email(),
            fake.phone_number(),
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.country(),
            fake.postcode()
        )
        customers.append(customer)
    
    if customers:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO sales.customers 
            (customer_name, email, phone, address, city, state, country, postal_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """
            inserted_ids = []
            for c in customers:
                cur.execute(sql, c)
                inserted_ids.append(cur.fetchone()[0])
            
            conn.commit()
            print(f"Added {len(customers)} new customers")
            return inserted_ids
    
    return []

def generate_products(conn, num_products=5):
    """Generate new products"""
    categories = ['Electronics', 'Furniture', 'Appliances', 'Clothing', 'Books', 'Sports']
    subcategories = {
        'Electronics': ['Phones', 'Computers', 'Audio', 'Accessories', 'Gaming'],
        'Furniture': ['Office', 'Living Room', 'Bedroom', 'Dining', 'Outdoor'],
        'Appliances': ['Kitchen', 'Laundry', 'Cleaning', 'Heating', 'Cooling'],
        'Clothing': ['Men', 'Women', 'Kids', 'Sportswear', 'Formal'],
        'Books': ['Fiction', 'Non-fiction', 'Educational', 'Comics', 'Magazines'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports']
    }
    
    products = []
    for _ in range(num_products):
        category = random.choice(categories)
        subcategory = random.choice(subcategories[category])
        price = round(random.uniform(10, 2000), 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)
        
        product = (
            fake.product_name(),
            category,
            subcategory,
            price,
            cost,
            random.randint(1, 5)
        )
        products.append(product)
    
    if products:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO sales.products 
            (product_name, category, subcategory, price, cost, supplier_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING product_id
            """
            inserted_ids = []
            for p in products:
                cur.execute(sql, p)
                inserted_ids.append(cur.fetchone()[0])
            
            conn.commit()
            print(f"Added {len(products)} new products")
            return inserted_ids
    
    return []

def get_existing_ids(conn):
    """Get existing customer and product IDs"""
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM sales.customers")
        customer_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT product_id FROM sales.products")
        product_ids = [row[0] for row in cur.fetchall()]
        
    return customer_ids, product_ids

def generate_orders(conn, num_orders=20, customer_ids=None, product_ids=None):
    """Generate new orders and order items"""
    if not customer_ids or not product_ids:
        customer_ids, product_ids = get_existing_ids(conn)
        
    if not customer_ids or not product_ids:
        print("No customers or products found. Please add some first.")
        return
    
    # Get last order date
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(order_date) FROM sales.orders")
        result = cur.fetchone()
        last_date = result[0] if result[0] else datetime.now() - timedelta(days=30)
    
    # Generate orders
    orders = []
    start_date = last_date + timedelta(days=1)
    end_date = datetime.now()
    
    for _ in range(num_orders):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        shipping_method = random.choice(['Standard', 'Express', 'Next Day'])
        shipping_cost = 0 if shipping_method == 'Standard' else random.choice([10, 15, 20])
        status = random.choice(['pending', 'processing', 'shipped', 'completed'])
        
        # For completed orders, ensure date is in the past
        if status == 'completed' and order_date > datetime.now() - timedelta(days=2):
            order_date = fake.date_time_between(start_date=start_date, end_date=datetime.now() - timedelta(days=2))
        
        orders.append((
            customer_id,
            order_date,
            status,
            shipping_method,
            shipping_cost,
            0  # Total amount will be updated after order items are created
        ))
    
    # Insert orders and create order items
    order_ids = []
    with conn.cursor() as cur:
        for order in orders:
            sql = """
            INSERT INTO sales.orders 
            (customer_id, order_date, status, shipping_method, shipping_cost, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING order_id
            """
            cur.execute(sql, order)
            order_id = cur.fetchone()[0]
            order_ids.append(order_id)
            
            # Generate 1-5 order items for each order
            num_items = random.randint(1, 5)
            order_total = 0
            
            # Select random products (ensuring no duplicates in the same order)
            order_products = random.sample(product_ids, min(num_items, len(product_ids)))
            
            for product_id in order_products:
                # Get product price
                cur.execute("SELECT price FROM sales.products WHERE product_id = %s", (product_id,))
                price = cur.fetchone()[0]
                
                quantity = random.randint(1, 5)
                discount = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
                item_total = price * quantity * (1 - discount)
                order_total += item_total
                
                # Insert order item
                cur.execute("""
                INSERT INTO sales.order_items 
                (order_id, product_id, quantity, unit_price, discount)
                VALUES (%s, %s, %s, %s, %s)
                """, (order_id, product_id, quantity, price, discount))
            
            # Update order total
            cur.execute("""
            UPDATE sales.orders 
            SET total_amount = %s
            WHERE order_id = %s
            """, (round(order_total + order[4], 2), order_id))
        
        conn.commit()
    
    print(f"Generated {len(order_ids)} new orders with items")

def update_existing_data(conn, num_updates=5):
    """Update some existing customers and products"""
    with conn.cursor() as cur:
        # Update some customers
        cur.execute("SELECT customer_id FROM sales.customers ORDER BY RANDOM() LIMIT %s", (num_updates,))
        customer_ids = [row[0] for row in cur.fetchall()]
        
        for customer_id in customer_ids:
            # Update a random field
            field = random.choice(['email', 'phone', 'address', 'city'])
            value = None
            
            if field == 'email':
                value = fake.email()
            elif field == 'phone':
                value = fake.phone_number()
            elif field == 'address':
                value = fake.street_address()
            elif field == 'city':
                value = fake.city()
            
            if value:
                cur.execute(f"""
                UPDATE sales.customers
                SET {field} = %s
                WHERE customer_id = %s
                """, (value, customer_id))
        
        # Update some products
        cur.execute("SELECT product_id FROM sales.products ORDER BY RANDOM() LIMIT %s", (num_updates,))
        product_ids = [row[0] for row in cur.fetchall()]
        
        for product_id in product_ids:
            # Update price (this will trigger SCD Type 2 in the DWH)
            price_change = random.uniform(0.9, 1.2)  # 10% decrease to 20% increase
            
            cur.execute("""
            UPDATE sales.products
            SET price = price * %s,
                cost = cost * %s
            WHERE product_id = %s
            """, (price_change, price_change, product_id))
        
        conn.commit()
    
    print(f"Updated {len(customer_ids)} customers and {len(product_ids)} products")

def main():
    """Main function to generate test data"""
    parser = argparse.ArgumentParser(description='Generate test data for sales database')
    parser.add_argument('--customers', type=int, default=5, help='Number of new customers to generate')
    parser.add_argument('--products', type=int, default=3, help='Number of new products to generate')
    parser.add_argument('--orders', type=int, default=20, help='Number of new orders to generate')
    parser.add_argument('--updates', type=int, default=3, help='Number of existing records to update')
    args = parser.parse_args()
    
    conn = connect_to_db()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    try:
        # Create schema if it doesn't exist
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS sales")
            
            # Create tables if they don't exist
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sales.customers (
                customer_id SERIAL PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                phone VARCHAR(20),
                address TEXT,
                city VARCHAR(50),
                state VARCHAR(50),
                country VARCHAR(50),
                postal_code VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sales.products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                category VARCHAR(50),
                subcategory VARCHAR(50),
                price DECIMAL(10, 2) NOT NULL,
                cost DECIMAL(10, 2),
                supplier_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sales.orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES sales.customers(customer_id),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending',
                shipping_method VARCHAR(50),
                shipping_cost DECIMAL(10, 2),
                total_amount DECIMAL(10, 2)
            )""")
            
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sales.order_items (
                order_item_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES sales.orders(order_id),
                product_id INTEGER REFERENCES sales.products(product_id),
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10, 2) NOT NULL,
                discount DECIMAL(5, 2) DEFAULT 0
            )""")
            
            conn.commit()
        
        # Generate new customers
        if args.customers > 0:
            customer_ids = generate_customers(conn, args.customers)
        else:
            customer_ids = []
        
        # Generate new products
        if args.products > 0:
            product_ids = generate_products(conn, args.products)
        else:
            product_ids = []
        
        # Generate new orders
        if args.orders > 0:
            existing_customers, existing_products = get_existing_ids(conn)
            # Use both new and existing IDs
            all_customers = existing_customers + customer_ids
            all_products = existing_products + product_ids
            generate_orders(conn, args.orders, all_customers, all_products)
        
        # Update existing data
        if args.updates > 0:
            update_existing_data(conn, args.updates)
        
        print("Data generation completed successfully!")
    except Exception as e:
        print(f"Error during data generation: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()