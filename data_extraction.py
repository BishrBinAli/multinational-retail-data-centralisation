import pandas as pd
from database_utils import DatabaseConnector


class DataExtractor:
    def read_rds_table(self, DBConnector, table_name):
        engine = DBConnector.db_engine
        with engine.connect() as con:
            df = pd.read_sql_table(table_name, con=con, index_col='index')
        return df


if __name__ == '__main__':
    DBConnector = DatabaseConnector('db_creds.yaml')
    DtExtractor = DataExtractor()
    table_names = DBConnector.list_db_tables()
    user_table_name = [table for table in table_names if 'user' in table][0]
    print(user_table_name)
    user_df = DtExtractor.read_rds_table(DBConnector, user_table_name)
