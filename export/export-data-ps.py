import psycopg2
import pandas as pd
import os

# PostgreSQL connection details
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5433
POSTGRES_DB = "postgres"
POSTGRES_USER = "essalhi"
POSTGRES_PASSWORD = "essalhi"

# Directory for storing CSV files
OUTPUT_DIR = "postgres_tables_csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# List of tables to export
TABLES_TO_EXPORT = [
    "Customer_Dim",      
    "Organization_Dim", 
    "Product_Dim",  
    "Order_Fact",
    "Geography_Dim",
    "Time_Dim"
]

def table_exists(conn, table_name):
    """Check if a table exists in the PostgreSQL database."""
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s
                AND table_schema = 'public'
            );
            """
            cursor.execute(query, (table_name,))
            return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error checking if table {table_name} exists: {e}")
        return False

def export_table_to_csv(table_name, output_dir, chunksize=10000):
    """Export a PostgreSQL table to a CSV file in chunks to avoid memory issues."""
    try:
        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        ) as conn:

            if not table_exists(conn, table_name):
                print(f"❌ Table {table_name} does not exist in the database.")
                return

            query = f'SELECT * FROM "{table_name}";'

            csv_file_path = os.path.join(output_dir, f"{table_name}.csv")

            # Use chunksize to read data in batches
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
                first_chunk = True
                for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
                    chunk.to_csv(f, index=False, header=first_chunk)
                    first_chunk = False  # Write header only for the first chunk

            print(f"✅ Exported {table_name} to {csv_file_path}")

    except Exception as e:
        print(f"Error exporting table {table_name}: {e}")

def main():
    """Export all specified tables to CSV files."""
    for table_name in TABLES_TO_EXPORT:
        export_table_to_csv(table_name, OUTPUT_DIR)

if __name__ == "__main__":
    main()
