import subprocess
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from io import StringIO  # Add this import

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path to Access database
access_db_path = "orion.mdb"

# MySQL connection configuration
mysql_config = {
    "host": "localhost",
    "user": "essalhi",
    "password": "essalhi",
    "database": "bi",
    "port": "3307"
}

# Connect to MySQL
try:
    mysql_conn = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor()
    logging.info("✅ Connected to MySQL database")
except Error as e:
    logging.error(f"❌ Failed to connect to MySQL: {e}")
    exit(1)

def get_access_tables():
    """Get a list of tables in the Access database."""
    try:
        result = subprocess.run(["mdb-tables", access_db_path], capture_output=True, text=True)
        tables = result.stdout.strip().split()
        logging.info(f"📋 Found tables in Access database: {tables}")
        return tables
    except Exception as e:
        logging.error(f"❌ Failed to get Access tables: {e}")
        return []

def get_table_columns(table_name):
    """Get a list of columns for a table in the Access database."""
    try:
        result = subprocess.run(["mdb-schema", access_db_path, "mysql"], capture_output=True, text=True)
        schema_lines = result.stdout.split("\n")
        
        columns = []
        inside_table = False

        for line in schema_lines:
            line = line.strip()
            if line.lower().startswith(f"create table `{table_name.lower()}`"):
                inside_table = True
            elif inside_table and line == ");":
                break
            elif inside_table and line:
                col_name = line.split()[0].strip("`")
                columns.append(col_name)

        logging.info(f"📋 Found columns for table {table_name}: {columns}")
        return columns
    except Exception as e:
        logging.error(f"❌ Failed to get columns for table {table_name}: {e}")
        return []

def create_mysql_table(table_name, columns):
    """Create a MySQL table with the given columns."""
    if not columns:
        logging.warning(f"⚠️ No columns found for table {table_name}. Skipping...")
        return

    try:
        col_defs = ", ".join([f"`{col}` TEXT" for col in columns])
        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_defs})"
        mysql_cursor.execute(create_stmt)
        mysql_conn.commit()
        logging.info(f"✅ Created table: {table_name}")
    except Error as e:
        logging.error(f"❌ Failed to create table {table_name}: {e}")

def transfer_table_data(table_name):
    """Transfer data from an Access table to a MySQL table."""
    try:
        result = subprocess.run(["mdb-export", access_db_path, table_name], capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"❌ Failed to export data from table {table_name}. Skipping...")
            return

        csv_data = StringIO(result.stdout)  # Use StringIO here
        df = pd.read_csv(csv_data)

        if df.empty:
            logging.warning(f"⚠️ Skipping empty table: {table_name}")
            return

        df = df.where(pd.notnull(df), None)

        # Prepare SQL query to insert data
        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        # Insert data row by row
        for _, row in df.iterrows():
            row_tuple = tuple(None if pd.isna(value) else value for value in row)
            mysql_cursor.execute(sql, row_tuple)

        mysql_conn.commit()
        logging.info(f"✅ Data transferred: {table_name}")
    except Error as e:
        logging.error(f"❌ Failed to transfer data for table {table_name}: {e}")

def remove_column_if_exists():
    """Remove the column '(' from all MySQL tables if it exists."""
    try:
        mysql_cursor.execute("SHOW TABLES")
        tables = [table[0] for table in mysql_cursor.fetchall()]

        for table in tables:
            mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = [col[0] for col in mysql_cursor.fetchall()]
            
            if "(" in columns:
                logging.warning(f"⚠️ Column '(' found in table {table}. Dropping it...")
                mysql_cursor.execute(f"ALTER TABLE `{table}` DROP COLUMN `(`")
                mysql_conn.commit()
                logging.info(f"✅ Column '(' dropped from table {table}")
            else:
                logging.info(f"✅ No column '(' found in table {table}")
    except Error as e:
        logging.error(f"❌ Failed to remove column '(': {e}")

# Main script
tables = get_access_tables()

if not tables:
    logging.error("❌ No tables found in the Access database.")
else:
    for table in tables:
        logging.info(f"🔄 Processing table: {table}")
        columns = get_table_columns(table)
        create_mysql_table(table, columns)
        transfer_table_data(table)

remove_column_if_exists()

mysql_cursor.close()
mysql_conn.close()

logging.info("🎉 Data migration completed successfully!")