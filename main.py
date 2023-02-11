# %%
import pandas as pd
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

# %%
if __name__ == '__main__':
    DBConnector = DatabaseConnector('db_creds.yaml')
    DtExtractor = DataExtractor()
    DtCleaner = DataCleaning()
    table_names = DBConnector.list_db_tables()
    user_table_name = [table for table in table_names if 'user' in table][0]
    user_df = DtExtractor.read_rds_table(DBConnector, user_table_name)
    cleaned_user_df = DtCleaner.clean_user_data(user_df)


# %%
