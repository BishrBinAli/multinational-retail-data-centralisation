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
