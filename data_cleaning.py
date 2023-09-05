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
        
        # drop any rows with unconverted, missing dates (unfathomable values in all columns)
        clean_df.dropna(subset=date_cols, inplace=True)

        # correct country code to match country name
        clean_df = self.correct_country_code(clean_df)
#        print(f"Missing values: {clean_df.isna().sum()}")

        # change dtype of the country and country_code columns to category
        clean_df = self.categorise_columns(clean_df, ["country", "country_code"])

        # clean email address values
        clean_df = self.clean_email_addresses(clean_df, "email_address")

        return clean_df



    def convert_date_column(self, df, col_name):

        date_format = "%Y-%m-%d"
        date_format2 = "%Y %B %d"
        date_format3 = "%B %Y %d"

        new_dates = pd.to_datetime(df[col_name], format=date_format, errors="coerce") \
                      .fillna(pd.to_datetime(df[col_name], format=date_format2, errors="coerce"))\
                      .fillna((pd.to_datetime(df[col_name], format=date_format3, errors="coerce")))
        
        print(f"Missing {col_name} dates: {new_dates.isna().sum()}")

        return new_dates


    def correct_country_code(self, df):
        """Check for rows where country and code do not match and correct country code"""

        country_code_map = { "United Kingdom": "GB", 
                             "Germany": "DE",
                             "United States": "US"}

        for country, code in country_code_map.items():
            df.loc[(df["country"] == country) & (df["country_code"] != code), "country_code"] = code

        return df
    
    def categorise_columns(self, df, col_names):
        """Convert dtype of columns in list to category"""

        for column in col_names:
            df[column] = df[column].astype("category")
        
        return df

        
    def clean_email_addresses(self, df, col_name="email_address"):
        """Removes double at symbol @@ in email addresses"""

        df[col_name] = df[col_name].str.replace("@@", "@")
        return df