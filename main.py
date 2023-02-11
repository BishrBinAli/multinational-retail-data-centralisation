# %%
import pandas as pd
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

# %%
if __name__ == '__main__':
    DBConnector_AWS = DatabaseConnector('db_creds.yaml')
    DtExtractor = DataExtractor()
    DtCleaner = DataCleaning()
    table_names = DBConnector_AWS.list_db_tables()
    user_table_name = [table for table in table_names if 'user' in table][0]
    user_df = DtExtractor.read_rds_table(DBConnector_AWS, user_table_name)
    cleaned_user_df = DtCleaner.clean_user_data(user_df)

    DBConnector_local = DatabaseConnector('db_creds_local.yaml')
    DBConnector_local.upload_to_db(cleaned_user_df, 'dim_users')

    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_df = DtExtractor.retrieve_pdf_data(pdf_link)



# %%
