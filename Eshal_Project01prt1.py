import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

"""


Put the file location you have for Warehouse_and_Retail_Sales.csv in the "" for extract_csv_data
Change password in DB_CONFIG to your password for mysql


"""
file_location = "C:/Users/Eshal/Downloads/Warehouse_and_Retail_Sales.csv"
# Database connection setup
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Passw0rd123",
    "database": "retail_data_mart"
}

def create_database():
    """Create the MySQL database and tables if they do not exist."""
    conn = mysql.connector.connect(host=DB_CONFIG["host"], user=DB_CONFIG["user"], password=DB_CONFIG["password"])
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS retail_data_mart;")
    conn.commit()
    cursor.close()
    conn.close()

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create Tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_dimension (
            date_id VARCHAR(6) PRIMARY KEY,
            year INT,
            month INT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(20) PRIMARY KEY,
            product_name VARCHAR(255),
            category VARCHAR(100)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id INT AUTO_INCREMENT PRIMARY KEY,
            supplier_name VARCHAR(255) UNIQUE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_transactions (
            transaction_id INT AUTO_INCREMENT PRIMARY KEY,
            date_id VARCHAR(6),
            product_id VARCHAR(20),
            supplier_id INT,
            retail_sales FLOAT,
            retail_transfers FLOAT,
            warehouse_sales FLOAT,
            FOREIGN KEY (date_id) REFERENCES date_dimension(date_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Database and tables created successfully.")

def extract_csv_data(file_path):
    """Extract sales data from CSV file."""
    return pd.read_csv(file_path)

def transform_data(df):
    """Transform data to match the data mart schema."""
    df["date_id"] = df["YEAR"].astype(str) + df["MONTH"].astype(str).str.zfill(2)
    df = df.rename(columns={
        "ITEM CODE": "product_id",
        "SUPPLIER": "supplier_name",
        "ITEM DESCRIPTION": "product_name",
        "ITEM TYPE": "category",
        "RETAIL SALES": "retail_sales",
        "RETAIL TRANSFERS": "retail_transfers",
        "WAREHOUSE SALES": "warehouse_sales"
    })
    return df[["date_id", "product_id", "supplier_name", "product_name", "category", "retail_sales", "retail_transfers", "warehouse_sales"]]

def load_to_mysql(df, table_name):
    """Load transformed data into MySQL tables."""
    engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded data into {table_name}.")

def insert_suppliers(df):
    """Ensure suppliers table exists and insert unique suppliers."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Ensure correct table structure
    cursor.execute("DROP TABLE IF EXISTS suppliers;")
    cursor.execute("""
        CREATE TABLE suppliers (
            supplier_id INT AUTO_INCREMENT PRIMARY KEY,
            supplier_name VARCHAR(255) UNIQUE
        );
    """)

    suppliers = df[["supplier_name"]].drop_duplicates().dropna()
    for supplier in suppliers["supplier_name"].values:
        cursor.execute("INSERT IGNORE INTO suppliers (supplier_name) VALUES (%s);", (supplier,))

    conn.commit()
    cursor.close()
    conn.close()
    print("Inserted unique suppliers into the database.")

def update_sales_with_supplier_id():
    """Ensure sales_transactions has supplier_id and update it."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Check and add supplier_id column if not exists
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_NAME = 'sales_transactions' AND COLUMN_NAME = 'supplier_id';
    """)
    if cursor.fetchone()[0] == 0:
        cursor.execute("ALTER TABLE sales_transactions ADD COLUMN supplier_id INT;")

    # Update sales_transactions to reference supplier_id
    cursor.execute("""
        UPDATE sales_transactions st
        JOIN suppliers s ON st.supplier_name = s.supplier_name
        SET st.supplier_id = s.supplier_id;
    """)

    # Drop supplier_name column after updating
    cursor.execute("ALTER TABLE sales_transactions DROP COLUMN supplier_name;")
    conn.commit()
    cursor.close()
    conn.close()
    print("Updated sales_transactions with supplier_id.")

def execute_query(query):
    """Execute a SQL query and print results."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()
    conn.close()

def populate_date_dimension():
    """Populate the date dimension table with unique year and month combinations."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Populate with 10 years of data (Modify as needed)
    for year in range(2015, 2026):
        for month in range(1, 13):
            date_id = f"{year}{str(month).zfill(2)}"
            cursor.execute(
                "INSERT IGNORE INTO date_dimension (date_id, year, month) VALUES (%s, %s, %s)",
                (date_id, year, month)
            )

    conn.commit()
    cursor.close()
    conn.close()
    print("Date dimension populated.")

# Create database and tables
create_database()

# Populate the date_dimension table
populate_date_dimension()

"""


Put the file location you have for Warehouse_and_Retail_Sales.csv in the "" for extract_csv_data


"""
# Extract data
sales_data = extract_csv_data(f'{file_location}')

# Transform data
sales_data = transform_data(sales_data)

# Insert unique suppliers
insert_suppliers(sales_data)

# Load sales data
load_to_mysql(sales_data, "sales_transactions")

# Update sales transactions to use supplier_id
update_sales_with_supplier_id()

# Execute queries
queries = {
    "Total Sales per Product Category": """
    SELECT p.category, SUM(st.retail_sales) AS total_retail_sales, SUM(st.warehouse_sales) AS total_warehouse_sales
    FROM sales_transactions st
    JOIN products p ON st.product_id = p.product_id
    GROUP BY p.category;
    """,
    "Top 5 Suppliers by Warehouse Sales": """
    SELECT s.supplier_name, SUM(st.warehouse_sales) AS total_warehouse_sales
    FROM sales_transactions st
    JOIN suppliers s ON st.supplier_id = s.supplier_id
    GROUP BY s.supplier_name
    ORDER BY total_warehouse_sales DESC
    LIMIT 5;
    """,
    "Monthly Sales Trends": """
    SELECT d.year, d.month, SUM(st.retail_sales + st.warehouse_sales) AS total_sales
    FROM sales_transactions st
    JOIN date_dimension d ON st.date_id = d.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """
}

print("Executing Queries:")
for title, query in queries.items():
    print(f"\n{title}:")
    execute_query(query)

print("ETL process and analysis completed!")
