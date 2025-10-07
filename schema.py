import mysql.connector
import pandas as pd
import os

from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.getenv("DB_HOST"),
    'port': int(os.getenv("DB_PORT")),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME")
}

def drop_tables(connection):
    """Drop all tables to start fresh"""
    cursor = connection.cursor()
    
    tables = [
        'INVOICE_ITEM', 'INVOICE', 'PRODUCT', 'PAYMENT', 
        'STORE', 'CUSTOMER', 'CATEGORY', 'BRAND'
    ]
    
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table {table}")
        except Exception as e:
            print(f"Error dropping table {table}: {e}")
    
    connection.commit()

def create_tables(connection):
    """Create tables with proper primary keys and constraints"""
    cursor = connection.cursor()
    
    tables = {
        'BRAND': """
            CREATE TABLE BRAND (
                BrandID INT PRIMARY KEY,
                BrandName VARCHAR(255),
                CategoryID INT
            )
        """,
        'CATEGORY': """
            CREATE TABLE CATEGORY (
                CategoryID INT PRIMARY KEY,
                CategoryName VARCHAR(255)
            )
        """,
        'CUSTOMER': """
            CREATE TABLE CUSTOMER (
                CustomerID INT PRIMARY KEY,
                CustomerName VARCHAR(255),
                Email VARCHAR(255),
                PhoneNumber VARCHAR(50)
            )
        """,
        'PAYMENT': """
            CREATE TABLE PAYMENT (
                PaymentID INT PRIMARY KEY,
                PaymentMethod VARCHAR(50)
            )
        """,
        'STORE': """
            CREATE TABLE STORE (
                StoreID INT PRIMARY KEY,
                Country VARCHAR(100)
            )
        """,
        'PRODUCT': """
            CREATE TABLE PRODUCT (
                StockCode VARCHAR(50),
                Description VARCHAR(150),
                UnitPrice DECIMAL(10,2),
                CategoryID INT,
                BrandID INT,
                PRIMARY KEY (StockCode, Description, CategoryID, BrandID)
            )
        """,
        'INVOICE': """
            CREATE TABLE INVOICE (
                InvoiceNo INT,
                CustomerID INT,
                InvoiceDate DATE,
                PaymentID INT,
                InvoiceStatus VARCHAR(50),
                StoreID INT,
                PRIMARY KEY (InvoiceNo, CustomerID, PaymentID, StoreID)
            )
        """,
        'INVOICE_ITEM': """
            CREATE TABLE INVOICE_ITEM (
                InvoiceNo INT,
                StockCode VARCHAR(50),
                Quantity INT,
                Revenue DECIMAL(10,2),
                PRIMARY KEY (InvoiceNo, StockCode)
            )
        """
    }
    
    for table_name, create_query in tables.items():
        try:
            cursor.execute(create_query)
            print(f"Table {table_name} created successfully")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
    
    connection.commit()

def upload_csv_to_table(connection, csv_file_path, table_name):
    """Upload CSV data to specified table with duplicate handling"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path)
        
        # Clean the data - handle NaN values
        df = df.fillna('')
        
        # Create cursor
        cursor = connection.cursor()
        
        # Generate INSERT query with IGNORE to skip duplicates
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.to_numpy()]
        
        # Insert data
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        
        print(f"Successfully uploaded {cursor.rowcount} rows to {table_name}")
        
    except Exception as e:
        print(f"Error uploading {csv_file_path} to {table_name}: {e}")
        connection.rollback()

def check_existing_data(connection):
    """Check if tables already have data"""
    cursor = connection.cursor()
    
    tables = ['BRAND', 'CATEGORY', 'CUSTOMER', 'PAYMENT', 'STORE', 'PRODUCT', 'INVOICE', 'INVOICE_ITEM']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"Table {table} has {count} rows")
        except Exception as e:
            print(f"Error checking table {table}: {e}")

def main():
    try:
        # Connect to database
        connection = mysql.connector.connect(**db_config)
        print("Connected to database successfully!")
        
        # Check existing data
        print("\nChecking existing data...")
        check_existing_data(connection)
        
        # Ask user what to do
        print("\nChoose an option:")
        print("1. Clear all data and re-upload everything")
        print("2. Skip existing data and only insert new records")
        print("3. Just check current data (no changes)")
        
        choice = input("Enter your choice (1, 2, or 3): ").strip()
        
        if choice == "1":
            # Drop and recreate tables
            print("\nDropping and recreating tables...")
            drop_tables(connection)
            create_tables(connection)
            
            # Upload all CSV files
            upload_all_data(connection)
            
        elif choice == "2":
            # Upload with IGNORE to skip duplicates
            print("\nUploading data (skipping duplicates)...")
            upload_all_data(connection)
            
        elif choice == "3":
            # Just check data
            print("\nCurrent data summary:")
            check_existing_data(connection)
        
        # Close connection
        connection.close()
        print("\nOperation completed successfully!")
        
    except Exception as e:
        print(f"Database connection failed: {e}")

def upload_all_data(connection):
    """Upload all CSV files"""
    csv_files = {
        'BRAND.csv': 'BRAND',
        'CATEGORY.csv': 'CATEGORY', 
        'CUSTOMER.csv': 'CUSTOMER',
        'PAYMENT.csv': 'PAYMENT',
        'STORE.csv': 'STORE',
        'PRODUCT.csv': 'PRODUCT',
        'INVOICE.csv': 'INVOICE',
        'INVOICE_ITEM.csv': 'INVOICE_ITEM'
    }
    
    # Upload each CSV file
    for csv_file, table_name in csv_files.items():
        file_path = os.path.join("artifact", csv_file)
        if os.path.exists(file_path):
            print(f"\nUploading {file_path} to {table_name}...")
            upload_csv_to_table(connection, file_path, table_name)
        else:
            print(f"File {file_path} not found!")

if __name__ == "__main__":
    main()