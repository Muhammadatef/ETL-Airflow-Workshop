#!/bin/bash

set -e
set -u

function create_user_and_database() {
	local database=$1
	echo "  Creating user and database '$database'"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
	    CREATE DATABASE $database;
	    GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
	echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
	for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
		create_user_and_database $db
	done
	echo "Multiple databases created"
fi

# Create schema and tables for sales_oltp
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "sales_oltp" <<-EOSQL
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS sales;
    
    -- Create tables
    CREATE TABLE sales.customers (
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
    );
    
    CREATE TABLE sales.products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50),
        subcategory VARCHAR(50),
        price DECIMAL(10, 2) NOT NULL,
        cost DECIMAL(10, 2),
        supplier_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE sales.orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES sales.customers(customer_id),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'pending',
        shipping_method VARCHAR(50),
        shipping_cost DECIMAL(10, 2),
        total_amount DECIMAL(10, 2)
    );
    
    CREATE TABLE sales.order_items (
        order_item_id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES sales.orders(order_id),
        product_id INTEGER REFERENCES sales.products(product_id),
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        discount DECIMAL(5, 2) DEFAULT 0
    );
    
    -- Insert sample data for customers
    INSERT INTO sales.customers (customer_name, email, phone, address, city, state, country, postal_code)
    VALUES
        ('John Doe', 'john.doe@example.com', '555-1234', '123 Main St', 'New York', 'NY', 'USA', '10001'),
        ('Jane Smith', 'jane.smith@example.com', '555-5678', '456 Elm St', 'Los Angeles', 'CA', 'USA', '90001'),
        ('Bob Johnson', 'bob.johnson@example.com', '555-9012', '789 Oak St', 'Chicago', 'IL', 'USA', '60601'),
        ('Alice Brown', 'alice.brown@example.com', '555-3456', '321 Pine St', 'Houston', 'TX', 'USA', '77001'),
        ('Charlie Wilson', 'charlie.wilson@example.com', '555-7890', '654 Maple St', 'Phoenix', 'AZ', 'USA', '85001');
    
    -- Insert sample data for products
    INSERT INTO sales.products (product_name, category, subcategory, price, cost, supplier_id)
    VALUES
        ('Laptop Pro', 'Electronics', 'Computers', 1299.99, 900.00, 1),
        ('Smartphone X', 'Electronics', 'Phones', 799.99, 500.00, 2),
        ('Office Chair', 'Furniture', 'Office', 199.99, 120.00, 3),
        ('Desk Lamp', 'Furniture', 'Lighting', 49.99, 25.00, 3),
        ('Coffee Maker', 'Appliances', 'Kitchen', 89.99, 45.00, 4),
        ('Bluetooth Headphones', 'Electronics', 'Audio', 149.99, 80.00, 2),
        ('Tablet Mini', 'Electronics', 'Computers', 399.99, 250.00, 1),
        ('Ergonomic Keyboard', 'Electronics', 'Accessories', 79.99, 40.00, 5),
        ('Wireless Mouse', 'Electronics', 'Accessories', 29.99, 15.00, 5),
        ('External Hard Drive', 'Electronics', 'Storage', 119.99, 70.00, 1);
    
    -- Insert sample data for orders
    INSERT INTO sales.orders (customer_id, order_date, status, shipping_method, shipping_cost, total_amount)
    VALUES
        (1, '2025-01-15 10:30:00', 'completed', 'Express', 15.00, 1314.99),
        (2, '2025-01-17 14:45:00', 'completed', 'Standard', 0.00, 799.99),
        (3, '2025-01-20 09:15:00', 'completed', 'Express', 20.00, 329.98),
        (4, '2025-01-22 16:20:00', 'shipped', 'Standard', 0.00, 89.99),
        (5, '2025-01-25 11:05:00', 'processing', 'Express', 15.00, 149.99),
        (1, '2025-02-03 13:40:00', 'completed', 'Standard', 0.00, 109.98),
        (2, '2025-02-07 10:10:00', 'shipped', 'Express', 15.00, 399.99),
        (3, '2025-02-10 15:30:00', 'pending', 'Standard', 0.00, 119.99),
        (4, '2025-02-15 09:45:00', 'processing', 'Express', 20.00, 229.98),
        (5, '2025-02-18 14:20:00', 'pending', 'Standard', 0.00, 79.99);
    
    -- Insert sample data for order_items
    INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price, discount)
    VALUES
        (1, 1, 1, 1299.99, 0.00),
        (2, 2, 1, 799.99, 0.00),
        (3, 3, 1, 199.99, 0.00),
        (3, 4, 1, 49.99, 0.00),
        (4, 5, 1, 89.99, 0.00),
        (5, 6, 1, 149.99, 0.00),
        (6, 8, 1, 79.99, 0.00),
        (6, 9, 1, 29.99, 0.00),
        (7, 7, 1, 399.99, 0.00),
        (8, 10, 1, 119.99, 0.00),
        (9, 8, 1, 79.99, 0.00),
        (9, 6, 1, 149.99, 0.00),
        (10, 8, 1, 79.99, 0.00);
EOSQL

# Create schema for sales_dwh
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "sales_dwh" <<-EOSQL
    -- Create DWH schema
    CREATE SCHEMA IF NOT EXISTS dwh;
EOSQL

echo "Initialization complete"