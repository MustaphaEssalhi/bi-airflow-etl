import logging
import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine


tables_mysql_source = [
    'city', 'continent', 'country', 'county', 'customer', 'customer_type', 'discount',
    'geo_type', 'order_item', 'orders', 'org_level', 'organization', 'postal_code',
    'price_list', 'product_level', 'product_list', 'state', 'street_code', 'supplier'
]


tables_postgresql_dataWarehouse = [
    'Customer_Dim', 'Organization_Dim', 'Order_Fact', 'Product_Dim', 'Geography_Dim', 'Time_Dim'
]


mysql_conn_id = 'mysql'
postgres_conn_id = 'postgres'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 26),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='bi-transform-completed',
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:


 
        
    @task(task_id='extract_table')
    def extract_table(table):
        try:
            mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
            sql = f"SELECT * FROM {table}"
            chunks = []
            for chunk in pd.read_sql(sql, mysql_hook.get_sqlalchemy_engine(), chunksize=10000):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
            print(f"Extracted data from {table}: {df.shape[0]} rows")
            return df.to_json(orient='split')
        except Exception as e:
            print(f"Error extracting data from MySQL for table {table}: {e}")
            return None
  
    extracted_data = {}
    for table in tables_mysql_source:
        extracted_data[table] = extract_table(table=table)


    @task(task_id='transform_and_load_customer_dim')
    def transform_and_load_customer_dim(customer_json):
        """
        Transforms and loads data into the Customer_Dim table in PostgreSQL.
        """
        if customer_json is None:
            return
        customer_df = pd.read_json(customer_json, orient='split')
        customer_df = customer_df.rename(columns={
            'customer_id': 'Customer_ID', 'country': 'Customer_Country', 'customer_type': 'Customer_Type',
            'gender': 'Customer_Gender', 'age_group': 'Customer_Age_Group', 'age': 'Customer_Age',
            'name': 'Customer_Name', 'firstname': 'Customer_Firstname', 'lastname': 'Customer_Lastname',
            'birth_date': 'Customer_Birth_Date'
        })
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = create_engine(postgres_hook.get_uri())
        customer_df.to_sql('Customer_Dim', engine, if_exists='replace', index=False)
        print("Customer_Dim loaded")
        
        
        

    @task(task_id='transform_and_load_organization_dim')
    def transform_and_load_organization_dim(organization_json):
        """
        Transforms and loads data into the Organization_Dim table in PostgreSQL.
        """
        if organization_json is None:
            return
        organization_df = pd.read_json(organization_json, orient='split')
        organization_df = organization_df.rename(columns={
            'employee_id': 'Employee_ID', 'country': 'Employee_Country', 'company': 'Company',
            'department': 'Department', 'section': 'Section', 'org_group': 'Org_Group',
            'job_title': 'Job_Title', 'name': 'Employee_Name', 'gender': 'Employee_Gender',
            'salary': 'Salary', 'birth_date': 'Employee_Birth_Date', 'hire_date': 'Employee_Hire_Date',
            'term_date': 'Employee_Term_Date'
        })
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = create_engine(postgres_hook.get_uri())
        organization_df.to_sql('Organization_Dim', engine, if_exists='replace', index=False)
        print("Organization_Dim loaded")


    @task(task_id='transform_and_load_order_fact')
    def transform_and_load_order_fact(orders_json, order_item_json, customer_json, organization_json, product_json, street_code_json):
        """
        Transforms and loads data into the Order_Fact table in PostgreSQL.
        """
        if orders_json is None or order_item_json is None or customer_json is None or organization_json is None or product_json is None or street_code_json is None:
            logging.warning("One or more input JSONs are None. Skipping transformation and load.")
            return

        try:
            orders_df = pd.read_json(StringIO(orders_json), orient='split')
            order_item_df = pd.read_json(StringIO(order_item_json), orient='split')
            customer_df = pd.read_json(StringIO(customer_json), orient='split')
            organization_df = pd.read_json(StringIO(organization_json), orient='split')
            product_df = pd.read_json(StringIO(product_json), orient='split')
            street_code_df = pd.read_json(StringIO(street_code_json), orient='split')

            print("orders_df columns:", orders_df.columns.tolist())
            print("order_item_df columns:", order_item_df.columns.tolist())
            print("customer_df columns:", customer_df.columns.tolist())
            print("street_code_df columns:", street_code_df.columns.tolist())

            if 'Order_ID' not in orders_df.columns:
                logging.error("orders_df does not contain the expected column: 'Order_ID'")
                raise ValueError("orders_df is missing the 'Order_ID' column")

            if 'Order_ID' not in order_item_df.columns:
                logging.error("order_item_df does not contain the expected column: 'Order_ID'")
                raise ValueError("order_item_df is missing the 'Order_ID' column")

            if 'Customer_ID' not in customer_df.columns:
                logging.error("customer_df does not contain the expected column: 'Customer_ID'")
                raise ValueError("customer_df is missing the 'Customer_ID' column")

            if 'Street_ID' not in street_code_df.columns:
                logging.error("street_code_df does not contain the expected column: 'Street_ID'")
                raise ValueError("street_code_df is missing the 'Street_ID' column")

            order_fact_df = pd.merge(orders_df, order_item_df, on='Order_ID')
            order_fact_df = pd.merge(order_fact_df, customer_df, on='Customer_ID')
            order_fact_df = pd.merge(order_fact_df, organization_df, on='Employee_ID')
            order_fact_df = pd.merge(order_fact_df, product_df, on='Product_ID')

            if 'Street_ID' in customer_df.columns:
                order_fact_df['street_code'] = customer_df['Street_ID']
            else:
                logging.error("customer_df does not contain the expected column: 'Street_ID'")
                raise ValueError("customer_df is missing the 'Street_ID' column")

            order_fact_df = pd.merge(
                order_fact_df,
                street_code_df,
                left_on='street_code',
                right_on='Street_ID',
                suffixes=('', '_street')
            )

            print("order_fact_df columns after merge:", order_fact_df.columns.tolist())

            if 'Street_ID' not in order_fact_df.columns:
                logging.error("Street_ID column is missing in order_fact_df after merge.")
                raise ValueError("Street_ID column is missing in order_fact_df after merge.")

            order_fact_df = order_fact_df.rename(columns={
                'Customer_ID': 'Customer_ID',
                'Employee_ID': 'Employee_ID',
                'Street_ID': 'Street_ID',  
                'Product_ID': 'Product_ID',
                'Order_Date': 'Order_Date',
                'Order_ID': 'Order_ID',
                'Order_Type': 'Order_Type',
                'Delivery_Date': 'Delivery_Date',
                'Quantity': 'Quantity',
                'Total_Retail_Price': 'Total_Retail_Price',
                'CostPrice_Per_Unit': 'Costprice_Per_Unit',
                'Discount': 'Discount'
            })

            order_fact_df = order_fact_df[['Customer_ID', 'Employee_ID', 'Street_ID', 'Product_ID', 'Order_Date', 'Order_ID', 'Order_Type', 'Delivery_Date', 'Quantity', 'Total_Retail_Price', 'Costprice_Per_Unit', 'Discount']]

            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = create_engine(postgres_hook.get_uri())
            order_fact_df.to_sql('Order_Fact', engine, if_exists='replace', index=False)
            logging.info("Order_Fact loaded successfully")

        except Exception as e:
            logging.error(f"Error transforming and loading Order_Fact: {e}")
            raise  
            
            
        
    @task(task_id='transform_and_load_product_dim')
    def transform_and_load_product_dim(product_list_json, product_level_json, supplier_json):
        """
        Transforms and loads data into the Product_Dim table in PostgreSQL.
        """
        if product_list_json is None or product_level_json is None or supplier_json is None:
            return
        try:
            product_list_df = pd.read_json(StringIO(product_list_json), orient='split')
            product_level_df = pd.read_json(StringIO(product_level_json), orient='split')
            supplier_df = pd.read_json(StringIO(supplier_json), orient='split')[['Supplier_ID', 'Supplier_Name', 'Country']] 

            product_dim_df = pd.merge(product_list_df, product_level_df, left_on='Product_Level', right_on='Product_Level')
            product_dim_df = pd.merge(product_dim_df, supplier_df, left_on='Supplier_ID', right_on='Supplier_ID')  

            product_dim_df = product_dim_df.rename(columns={
                'Product_ID': 'Product_ID',
                'Product_Name': 'Product_Name',
                'Supplier_ID': 'Supplier_ID',
                'Product_Level': 'Product_Level',
                'Product_Ref_ID': 'Product_Ref_ID',
                'Supplier_Name': 'Supplier_Name', 
                'Country': 'Supplier_Country'  
            })

            product_dim_df = product_dim_df[['Product_ID', 'Product_Name', 'Supplier_ID', 'Product_Level', 'Product_Ref_ID', 'Supplier_Country', 'Supplier_Name']]

            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = create_engine(postgres_hook.get_uri())
            product_dim_df.to_sql('Product_Dim', engine, if_exists='replace', index=False)
            print("Product_Dim loaded successfully")
        except Exception as e:
            print(f"Error transforming and loading Product_Dim: {e}")
            
            
            
    @task(task_id='transform_and_load_geography_dim')
    def transform_and_load_geography_dim(street_code_json, city_json, continent_json, country_json, state_json):
        """
        Transforms and loads data into the Geography_Dim table in PostgreSQL.
        """
        if street_code_json is None or city_json is None or continent_json is None or country_json is None or state_json is None:
            logging.warning("One or more input JSONs are None. Skipping transformation and load.")
            return

        try:
            street_code_df = pd.read_json(StringIO(street_code_json), orient='split')
            city_df = pd.read_json(StringIO(city_json), orient='split')
            continent_df = pd.read_json(StringIO(continent_json), orient='split')
            country_df = pd.read_json(StringIO(country_json), orient='split')
            state_df = pd.read_json(StringIO(state_json), orient='split')

            # Debug
            print("street_code_df columns:", street_code_df.columns.tolist())
            print("city_df columns:", city_df.columns.tolist())
            print("country_df columns:", country_df.columns.tolist())
            print("state_df columns:", state_df.columns.tolist())

            # Verify the columns in city_df
            if not all(col in city_df.columns for col in ['City_ID', 'City_Name', 'Country']):
                logging.error("city_df does not contain the expected columns: 'City_ID', 'City_Name', 'Country'")
                return

            # Verify the columns in continent_df
            if not all(col in continent_df.columns for col in ['Continent_ID', 'Continent_Name']):
                logging.error("continent_df does not contain the expected columns: 'Continent_ID', 'Continent_Name'")
                return

            # Verify the columns in street_code_df
            if not all(col in street_code_df.columns for col in ['Street_ID', 'Country', 'Street_Name', 'City_ID', 'Postal_Code']):
                logging.error("street_code_df does not contain the expected columns: 'Street_ID', 'Country', 'Street_Name', 'City_ID', 'Postal_Code'")
                return

            # Verify the columns in country_df
            if not all(col in country_df.columns for col in ['Country', 'Country_ID', 'Continent_ID']):
                logging.error("country_df does not contain the expected columns: 'Country', 'Country_ID', 'Continent_ID'")
                return

            # Verify the columns in state_df
            if not all(col in state_df.columns for col in ['State_ID', 'State_Type', 'State_Code', 'State_Name', 'Country']):
                logging.error("state_df does not contain the expected columns: 'State_ID', 'State_Type', 'State_Code', 'State_Name', 'Country'")
                return

            # Select specific columns
            city_df = city_df[['City_ID', 'City_Name', 'Country']]
            continent_df = continent_df[['Continent_ID', 'Continent_Name']]
            street_code_df = street_code_df[['Street_ID', 'Country', 'Street_Name', 'City_ID', 'Postal_Code']]
            country_df = country_df[['Country', 'Country_ID', 'Continent_ID']] 
            state_df = state_df[['State_ID', 'State_Code', 'State_Name', 'Country']]

            # Create a mapping between Country (codes) and Country_ID (numeric IDs)
            country_mapping = country_df.set_index('Country')['Country_ID'].to_dict()

            # Debug: Print the country mapping
            print("Country mapping:", country_mapping)

            street_code_df['Country_ID'] = street_code_df['Country'].map(country_mapping)
            state_df['Country_ID'] = state_df['Country'].map(country_mapping)

            print("Unique values in street_code_df['Country_ID']:", street_code_df['Country_ID'].unique())
            print("Unique values in state_df['Country_ID']:", state_df['Country_ID'].unique())

            # Rename the 'Country' column in each DataFrame to avoid conflicts during merge
            street_code_df = street_code_df.rename(columns={'Country': 'Street_Country'})
            city_df = city_df.rename(columns={'Country': 'City_Country'})
            state_df = state_df.rename(columns={'Country': 'State_Country'})

            # Merge DataFrames
            geo_dim_df = pd.merge(street_code_df, city_df, left_on='City_ID', right_on='City_ID')
            geo_dim_df = pd.merge(geo_dim_df, country_df, left_on='Country_ID', right_on='Country_ID')  # Merge using Country_ID
            geo_dim_df = pd.merge(geo_dim_df, continent_df, left_on='Continent_ID', right_on='Continent_ID')  # Merge using Continent_ID
            geo_dim_df = pd.merge(geo_dim_df, state_df, left_on='Country_ID', right_on='Country_ID')  # Merge using Country_ID

            # Rename columns
            geo_dim_df = geo_dim_df.rename(columns={
                'Street_ID': 'Street_ID',
                'Continent_Name': 'Continent',
                'City_Name': 'City',
                'Street_Name': 'Street_Name',
                'Postal_Code': 'Postal_Code',
                'Country_Name': 'Country',
                'State_Name': 'State',
                'State_Code': 'State_Code',
                'State_ID': 'State_ID'
            })

            # Select final columns
            geo_dim_df = geo_dim_df[['Street_ID', 'Continent', 'Country', 'State_ID', 'State_Code', 'State', 'Postal_Code', 'City', 'Street_Name']]

            # Load data into PostgreSQL
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = create_engine(postgres_hook.get_uri())
            geo_dim_df.to_sql('Geography_Dim', engine, if_exists='replace', index=False)
            logging.info("Geography_Dim loaded successfully")

        except Exception as e:
            logging.error(f"Error transforming and loading Geography_Dim: {e}")
        
    

    @task(task_id='transform_and_load_time_dim')
    def transform_and_load_time_dim():
        """
        Generates and loads data into the Time_Dim table in PostgreSQL.
        """
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        date_range = pd.date_range(start=start_date, end=end_date)
        time_dim_df = pd.DataFrame(date_range, columns=['Date_ID'])
        time_dim_df['Year_ID'] = time_dim_df['Date_ID'].dt.year.astype(str)
        time_dim_df['Quarter'] = time_dim_df['Date_ID'].dt.quarter.apply(lambda x: f"Q{x}")
        time_dim_df['Month_Name'] = time_dim_df['Date_ID'].dt.strftime('%B')
        time_dim_df['Weekday_Name'] = time_dim_df['Date_ID'].dt.strftime('%A')
        time_dim_df['Month_Num'] = time_dim_df['Date_ID'].dt.month
        time_dim_df['Weekday_Num'] = time_dim_df['Date_ID'].dt.weekday
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = create_engine(postgres_hook.get_uri())
        time_dim_df.to_sql('Time_Dim', engine, if_exists='replace', index=False)
        print("Time_Dim loaded")

    # Task Dependencies
    customer_task = transform_and_load_customer_dim(extracted_data['customer'])
    org_task = transform_and_load_organization_dim(extracted_data['organization'])
    order_task = transform_and_load_order_fact(extracted_data['orders'], extracted_data['order_item'], extracted_data['customer'], extracted_data['organization'], extracted_data['product_list'], extracted_data['street_code'])
    product_task = transform_and_load_product_dim(extracted_data['product_list'], extracted_data['product_level'], extracted_data['supplier'])
    geo_task = transform_and_load_geography_dim(extracted_data['street_code'], extracted_data['city'], extracted_data['continent'], extracted_data['country'], extracted_data['state'])
    time_task = transform_and_load_time_dim()

    
    customer_task >> org_task >> order_task
    product_task >> order_task
    geo_task >> order_task
    order_task >> time_task 