# %%
import pandas as pd
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

# %%
if __name__ == '__main__':
    # %%
    DBConnector_AWS = DatabaseConnector('db_creds.yaml')
    DBConnector_local = DatabaseConnector('db_creds_local.yaml')
    DtExtractor = DataExtractor()
    DtCleaner = DataCleaning()
    table_names = DBConnector_AWS.list_db_tables()
    user_table_name = [table for table in table_names if 'user' in table][0]
    orders_table_name = [table for table in table_names if 'orders' in table][0]

    # %%
    # Getting user details from AWS database and cleaning the data
    user_df = DtExtractor.read_rds_table(DBConnector_AWS, user_table_name)
    cleaned_user_df = DtCleaner.clean_user_data(user_df)
    # Uploading user details to local database
    DBConnector_local.upload_to_db(cleaned_user_df, 'dim_users')

    # %%
    # Getting card details from pdf link and cleaning it
    card_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_df = DtExtractor.retrieve_pdf_data(card_pdf)
    cleaned_card_df = DtCleaner.clean_card_data(card_df)
    # Uploading card details to local database
    DBConnector_local.upload_to_db(cleaned_card_df, 'dim_card_details')

    # %%
    # Getting store_details from an api and cleaning it
    api_headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = DtExtractor.list_number_of_stores(num_stores_endpoint,api_headers)

    store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    stores_df = DtExtractor.retrieve_stores_data(store_details_endpoint, api_headers, number_of_stores)
    
    cleaned_stores_df = DtCleaner.clean_store_data(stores_df)

    # Uploading store details to local database
    DBConnector_local.upload_to_db(cleaned_stores_df, 'dim_store_details')


    # %%
    # Getting product details from an AWS S3 bucket
    file_s3_address = "s3://data-handling-public/products.csv"
    products_df = DtExtractor.extract_from_s3(file_s3_address)
    # Converting product weights to kg
    cleaned_products_df = DtCleaner.convert_product_weights(products_df)
    # Cleaning products_data
    cleaned_products_df = DtCleaner.clean_products_data(cleaned_products_df)
    # Uploading product details to local database
    DBConnector_local.upload_to_db(cleaned_products_df, 'dim_products')


    # %%
    # Getting orders data from AWS database and cleaning it
    orders_df = DtExtractor.read_rds_table(DBConnector_AWS, orders_table_name)
    cleaned_orders_df = DtCleaner.clean_orders_data(orders_df)
    # Uploading orders data to local database
    DBConnector_local.upload_to_db(cleaned_orders_df, 'orders_table')


    # %%
    # Getting and cleaning date events data from json file stored in S3
    file_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events_df = DtExtractor.extract_json_from_url(file_url)
    cleaned_date_events_df = DtCleaner.clean_date_events_data(date_events_df)
    # Uploading date events data to local database
    DBConnector_local.upload_to_db(cleaned_date_events_df, 'dim_date_times')


    # %%
    # Changing orders_table data types
    card_number_length = cleaned_orders_df['card_number'].apply(lambda x: len(str(x))).max()
    store_code_length = cleaned_orders_df['store_code'].apply(lambda x: len(str(x))).max()
    product_code_length = cleaned_orders_df['product_code'].apply(lambda x: len(str(x))).max()
    # In postgreSQL
    # SELECT MAX(LENGTH(card_number)), MAX(LENGTH(store_code)), MAX(LENGTH(product_code)) FROM orders_table
    orders_new_types = {
        'date_uuid': 'UUID',
        'user_uuid': 'UUID',
        'card_number': f'VARCHAR({card_number_length})',
        'store_code': f'VARCHAR({store_code_length})',
        'product_code': f'VARCHAR({product_code_length})',
        'product_quantity': 'SMALLINT'
    }
    DBConnector_local.change_column_types('orders_table', orders_new_types)

    # %%
    # Changing column data types of dim_users table
    country_code_length = cleaned_user_df['country_code'].apply(lambda x: len(str(x))).max()
    # In postgreSQL
    # SELECT MAX(LENGTH(country_code)) FROM dim_users
    users_new_types = {
        'first_name': 'VARCHAR(255)',
        'last_name': 'VARCHAR(255)',
        'date_of_birth': 'DATE',
        'country_code': f'VARCHAR({country_code_length})',
        'user_uuid': 'UUID',
        'join_date': 'DATE'
    }
    DBConnector_local.change_column_types('dim_users', users_new_types)

    # %%
    # Changing column data types of dim_store_details table
    # Merging two latitute columns into one in dim_store_details
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """UPDATE dim_store_details SET latitude = COALESCE(latitude, lat);
    ALTER TABLE dim_store_details DROP COLUMN lat;"""
        con.execute(sql_statement)
    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = "SELECT MAX(LENGTH(store_code)) FROM dim_store_details"
        store_code_length = con.execute(sql_statement).fetchall()[0][0]
        sql_statement = "SELECT MAX(LENGTH(country_code)) FROM dim_store_details"
        country_code_length = con.execute(sql_statement).fetchall()[0][0]
    # So as to be able to enter N/A
    country_code_length = max(country_code_length, 3)
    # %%
    store_new_types = {
        'longitude': 'FLOAT',
        'locality': 'VARCHAR(255)',
        'store_code': f'VARCHAR({store_code_length})',
        'staff_numbers': 'SMALLINT',
        'opening_date': 'DATE',
        'store_type': 'VARCHAR(255)',
        'latitude': 'FLOAT',
        'country_code': f'VARCHAR({country_code_length})',
        'continent': 'VARCHAR(255)'
    }
    DBConnector_local.change_column_types('dim_store_details', store_new_types)
    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """UPDATE dim_store_details 
        SET 
            address = COALESCE(address, 'N/A'),
            locality = COALESCE(locality, 'N/A'),
            country_code = COALESCE(country_code, 'N/A'),
            continent = COALESCE(continent, 'N/A')
        WHERE store_type = 'Web Portal';"""
        con.execute(sql_statement)


    
    # Changing data types of dim_products table
    # %%
    # Removing '£' from price
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """UPDATE dim_products 
        SET product_price = REPLACE(product_price, '£', '');"""
        # Also, TRIM('£' FROM product_price)
        con.execute(sql_statement)
    # %%
    # Creating new column 'weight class'
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        ALTER TABLE dim_products
        ADD COLUMN weight_class VARCHAR(14);
        UPDATE dim_products 
        SET weight_class = CASE
            WHEN weight < 3 THEN 'Light'
            WHEN weight BETWEEN 3 AND 40 THEN 'Mid_Sized'
            WHEN weight BETWEEN 41 AND 140 THEN 'Heavy'
            WHEN weight > 140 THEN 'Truck_Required'
        END;"""
        con.execute(sql_statement)
    # %%
    # Changing removed column to still_available
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        ALTER TABLE dim_products
        RENAME COLUMN removed TO still_available;
        UPDATE dim_products 
        SET still_available = CASE
            WHEN still_available = 'Removed' THEN FALSE
	        WHEN still_available = 'Still_avaliable' THEN TRUE
        END;
        """
        con.execute(sql_statement)
    # %%
    products_new_types = {
        'product_price': 'FLOAT',
        'weight': 'FLOAT',
        '"EAN"': 'VARCHAR(17)',
        # for column values, have to use double quotes to use it as they are in SQL, single quotes doesnt work
        'product_code': 'VARCHAR(11)',
        'date_added': 'DATE',
        'uuid': 'UUID',
        'still_available': 'BOOLEAN',
        'weight_class': 'VARCHAR(14)'
    }
    DBConnector_local.change_column_types('dim_products', products_new_types)
    
    # Changing data types of dim_date_times table
    # %%
    date_new_types = {
        'month': 'CHAR(2)',
        'year': 'CHAR(4)',
        'day': 'CHAR(2)',
        'time_period': 'VARCHAR(10)',
        'date_uuid': 'UUID'
    }
    DBConnector_local.change_column_types('dim_date_times', date_new_types)
    
    # Changing data types of dim_card_details table
    # %%
    card_new_types = {
        'card_number': 'VARCHAR(19)',
        'expiry_date': 'VARCHAR(5)',
        'date_payment_confirmed': 'DATE'
    }
    DBConnector_local.change_column_types('dim_card_details', card_new_types)
    
    # Adding primary keys to the tables
    # %%
    # dim_card_details - card_number
    # dim_date_times - date_uuid
    # dim_products - product_code
    # dim_store_details - store_code
    # dim_users - user_uuid
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        ALTER TABLE dim_card_details
        ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);
        ALTER TABLE dim_date_times
        ADD CONSTRAINT dim_date_details_pk PRIMARY KEY (date_uuid);
        ALTER TABLE dim_products
        ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);
        ALTER TABLE dim_store_details
        ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);
        ALTER TABLE dim_users
        ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);
        """
        con.execute(sql_statement)
# %%
