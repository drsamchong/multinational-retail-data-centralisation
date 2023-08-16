import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


if __name__ == "__main__":
    db_conn = DatabaseConnector()
    #db_creds = db_conn.read_db_creds()
    #db_conn.list_db_tables(engine)
    #engine = db_conn.init_db_engine(db_creds)

    data_ext = DataExtractor()
    users_df = data_ext.read_rds_table(db_conn, "legacy_users")

    print(users_df.head())