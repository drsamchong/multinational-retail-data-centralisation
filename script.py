import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def process_user_data():

    rds_conn = DatabaseConnector("db_creds.yaml")
    data_ext = DataExtractor()

    users_df = data_ext.read_rds_table(rds_conn, "legacy_users")
    # print("Unclean!")
    # print(users_df.head())

    user_cleaner = DataCleaning()
    cleaned_user_df = user_cleaner.clean_user_data(users_df)

    print("Cleaned data:\n")
    print(cleaned_user_df.head(10))
    print(cleaned_user_df.info())

    return cleaned_user_df

def save_cleaned_data(clean_df, table_name):

    local_conn = DatabaseConnector("local_db_creds.yaml")
    local_conn.upload_to_db(clean_df, table_name)


def process_card_data():
    data_ext = DataExtractor()

    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_data = data_ext.retrieve_pdf_data(pdf_url)

    card_cleaner = DataCleaning()
    cleaned_card_df = card_cleaner.clean_card_data(card_data)

    return cleaned_card_df




def main():

    cleaned_user_df = process_user_data()
    save_cleaned_data(cleaned_user_df, "dim_users")
    cleaned_card_data = process_card_data()
#    cleaned_card_data.to_csv("cleaned_card_transactions.csv", index=False)
    save_cleaned_data(cleaned_card_data, "dim_card_details")




if __name__ == "__main__":
    main()