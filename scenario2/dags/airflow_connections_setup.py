#!/usr/bin/env python
"""
Script to set up the Airflow connection for Postgres
Adjust this script to your container environment
"""

from airflow.models import Connection
from airflow.utils.db import create_session
import os

def setup_connections():
    """Set up the Postgres connection in Airflow"""
    
    # Create Postgres connection
    postgres_conn = Connection(
        conn_id='postgres_conn',
        conn_type='postgres',
        host='postgres',       # Container name in docker network
        schema='mydatabase',   # Database name as defined in docker-compose
        login='postgres',      # Username as defined in docker-compose
        password='postgres',   # Password as defined in docker-compose
        port=5432            # Internal port (not the mapped port)
    )
    
    # Add connection to the Airflow DB
    with create_session() as session:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == postgres_conn.conn_id
        ).first()
        
        if existing_conn:
            print(f"Connection {postgres_conn.conn_id} already exists. Updating...")
            existing_conn.host = postgres_conn.host
            existing_conn.schema = postgres_conn.schema
            existing_conn.login = postgres_conn.login
            existing_conn.password = postgres_conn.password
            existing_conn.port = postgres_conn.port
        else:
            print(f"Creating new connection: {postgres_conn.conn_id}")
            session.add(postgres_conn)
        
        session.commit()
    
    print("Airflow connection setup completed successfully")

if __name__ == "__main__":
    setup_connections()
