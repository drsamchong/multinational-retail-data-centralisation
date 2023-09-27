import pandas as pd
import tabula
import requests

class DataExtractor():
    """ A utility class containing methods for extracting data from a variety of source """
    def read_rds_table(self, db_conn, table_name):

        engine = db_conn.init_db_engine()
        db_conn.list_db_tables(engine)


        df = pd.read_sql_table(table_name, con=engine.connect())
        return df
    

    def retrieve_pdf_data(self, pdf_path):

        pdf_dfs = tabula.read_pdf(pdf_path, pages="all", stream=True)
        return pdf_dfs


    def list_number_of_stores(self, url, headers):

        response = requests.get(url, headers=headers)
        n_stores = int(response.json()["number_stores"])
        return n_stores
    
    def retrieve_stores_data(self, url):

        root_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/"
        store_path = "prod/store_details/"
        n_store_path = "prod/number_stores"

        headers = {
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }       

        n_stores = self.list_number_of_stores(root_url+n_store_path, headers)


        store_df = pd.DataFrame()
        for store in range(n_stores):
            print(f"getting store {store}")
            store_endpoint = f"{url}{str(store)}"
            response = requests.get(store_endpoint, headers=headers)
            store_data = response.json()
            store_df = pd.concat([store_df, pd.DataFrame(store_data, index=[store_data["index"]])])
        
        return store_df
    

if __name__ == "__main__":

    de = DataExtractor()
    store_info_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    store_data = de.retrieve_stores_data(store_info_url)
    print(store_data.info())
    store_data.to_csv("store_data.csv")






