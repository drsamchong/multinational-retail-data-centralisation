import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def main():
    rds_conn = DatabaseConnector("db_creds.yaml")
    data_ext = DataExtractor()

    users_df = data_ext.read_rds_table(rds_conn, "legacy_users")
    print("Unclean!")
    print(users_df.head())

    cleaner = DataCleaning()
    cleaned_df = cleaner.clean_user_data(users_df)
    print(cleaned_df.head(10))





if __name__ == "__main__":
    main()