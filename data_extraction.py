import pandas as pd
import tabula

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


