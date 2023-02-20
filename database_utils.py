import yaml
from sqlalchemy import create_engine, inspect
import sqlalchemy


class DatabaseConnector:
    def __init__(self, yaml_file) -> None:
        self.yaml_file = yaml_file
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def read_db_creds(self) -> dict:
        with open(self.yaml_file) as f:
            db_creds = yaml.safe_load(f)
        return db_creds

    def init_db_engine(self):
        connection_url = sqlalchemy.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=self.db_creds['RDS_USER'],
            password=self.db_creds['RDS_PASSWORD'],
            host=self.db_creds['RDS_HOST'],
            database=self.db_creds['RDS_DATABASE'],
            port=self.db_creds['RDS_PORT']
        )
        engine = create_engine(connection_url)
        return engine

    def list_db_tables(self) -> list:
        inspector = inspect(self.db_engine)
        db_tables = inspector.get_table_names()
        print(db_tables)
        return db_tables

    def upload_to_db(self, df, table_name) -> None:
        with self.db_engine.connect() as con:
            df.to_sql(table_name, con, if_exists='replace', index=False)

    def change_column_types(self, table_name, new_types) -> None:
        """
        new_types: dictionary with column name as key and new type as value
        """
        sql_stmt = f'ALTER TABLE {table_name}'
        for key in new_types:
            sql_stmt = sql_stmt + f' ALTER COLUMN {key} TYPE {new_types[key]} USING {key}::{new_types[key]},'
        sql_stmt = sql_stmt[:-1]
        with self.db_engine.connect() as con:
            con.execute(sql_stmt)




if __name__ == "__main__":
    DB = DatabaseConnector('db_creds.yaml')
    DB.list_db_tables()
