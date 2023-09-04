import pandas as pd
import numpy as np

class DataCleaning():
    """ Contains methods to clean data from a variety of data sources """
    
    def clean_user_data(self, df):

        # Missing values appear to be represented as a string "NULL" rather than None or np.nan, so replacing
        clean_df = df.replace("NULL", np.nan)

        # Any rows with na contain no values in any columns, so drop the rows
        clean_df.dropna(inplace=True)

        # date_of_birth, join_date object to datetime
        date_cols = ["date_of_birth", "join_date"]
        for col in date_cols:
            clean_df.loc[:, col] = self.convert_date_column(clean_df, col)

        return clean_df



    def convert_date_column(self, df, col_name):

        date_format = "%Y-%m-%d"
        date_format2 = "%Y %B %d"
        date_format3 = "%B %Y %d"

        new_dates = pd.to_datetime(df[col_name], format=date_format, errors="coerce") \
                      .fillna(pd.to_datetime(df[col_name], format=date_format2, errors="coerce"))\
                      .fillna((pd.to_datetime(df[col_name], format=date_format3, errors="coerce")))

        return new_dates
