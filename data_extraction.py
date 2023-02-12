import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests


class DataExtractor:
    def read_rds_table(self, DBConnector, table_name):
        engine = DBConnector.db_engine
        with engine.connect() as con:
            df = pd.read_sql_table(table_name, con=con, index_col='index')
        return df

    def retrieve_pdf_data(self, pdf_link):
        df_list = tabula.read_pdf(pdf_link, pages='all')
        pdf_df = pd.concat(df_list)
        return pdf_df

    def list_number_of_stores(self, api_endpoint, headers):
        response = requests.get(api_endpoint, headers=headers)
        response_data = response.json()
        number_of_stores = response_data['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, endpoint_template, headers, num_stores):
        stores_data_list = []
        for i in range(num_stores):
            api_endpoint = endpoint_template.format(store_number=i)
            response = requests.get(api_endpoint, headers=headers)
            stores_data_list.append(response.json())
        stores_data_df = pd.DataFrame(stores_data_list)
        stores_data_df.set_index('index', inplace=True)
        return stores_data_df



if __name__ == '__main__':
    DBConnector = DatabaseConnector('db_creds.yaml')
    DtExtractor = DataExtractor()
    table_names = DBConnector.list_db_tables()
    user_table_name = [table for table in table_names if 'user' in table][0]
    print(user_table_name)
    user_df = DtExtractor.read_rds_table(DBConnector, user_table_name)
