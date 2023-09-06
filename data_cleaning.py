import pandas as pd
import numpy as np

class DataCleaning():
    """ Contains methods to clean data from a variety of data sources """
    
    def clean_user_data(self, df):

        # Missing values appear to be represented as a string "NULL" rather than None or np.nan, so replacing
        clean_df = df.replace("NULL", np.nan)

        # Any rows with na contain no values in any columns, so drop the rows
        clean_df.dropna(inplace=True)

        # change dtypes of date_of_birth, join_date object to datetime
        date_cols = ["date_of_birth", "join_date"]
        for col in date_cols:
            clean_df[col] = self.convert_date_column(clean_df[col])
        
        # drop any rows with unconverted, missing dates (unfathomable values in all columns)
        clean_df.dropna(subset=date_cols, inplace=True)

        # correct country code to match country name
        clean_df = self.correct_country_code(clean_df)

        # clean email address values
        clean_df = self.clean_email_addresses(clean_df, "email_address")

        # clean phone_numbers
        clean_df = self.clean_phone_numbers(clean_df)

        # Set address case to title (capitalise each word)
        clean_df["address"] = self.fix_address_case(clean_df["address"])


        # change dtype of the country and country_code columns to category - this has to be done after phone_numbers because 
        # apply seems to remove the category type from the columns
        clean_df = self.categorise_columns(clean_df, ["country", "country_code"])

        # Clean old index column and reset
        clean_df = self.tidy_index(clean_df) 

        return clean_df



#    def convert_date_column(self, df, col_name):
    def convert_date_column(self, date_col):
        """Change dtype of column to datetime"""

        new_dates = pd.to_datetime(date_col, infer_datetime_format=True, errors="coerce")
#        print(f"Missing {col_name} dates: {new_dates.isna().sum()}")
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


    def clean_phone_numbers(self, df):
        """Formats phones numbers as close to E.164 standard as possible without "+" prefix to store as int"""
        
        # Target format is international without + prefix to dialling code

        # extract out any phone extension to new column
        df[["phone_number", "phone_ext"]] = df["phone_number"].str.split("x", expand=True)

        # remove (0) area code. Include regex argument to avoid FutureWarning
        df["phone_number"] = df["phone_number"].str.replace("(0)", "", regex=False)

        # remove other non-numeric characters
        df["phone_number"] = df["phone_number"].str.findall("\d+").apply(lambda n: "".join(map(str, n)))

        # remove leading zeroes
        df["phone_number"] = df["phone_number"].str.lstrip("0")

        # add country's dialling code if missing
        df = df.apply(lambda x: self.check_dialling_code(x), axis=1)

        # change dtype pf phone_number and ext to int
        phone_col = ["phone_number", "phone_ext"]
        for col in phone_col:
            df[col] = self.change_column_to_int(df[col])

        return df
    
    def change_column_to_int(self, col):
        """Change dtype of column to Int64"""

        # Use Int64 to allow for None in phone ext
        col = col.astype("Int64")
        return col
    

    def check_dialling_code(self, row):
        """
        Helper function checks the beginning of the phone number has the correct dialling code based on country code
        and adds if not.
        """

    
        dialling_codes = {"GB": "44",
                        "US": "1",
                        "DE": "49"}

        country = row["country_code"]
        code = dialling_codes[country]
        
        # if code is present, remove and then strip any zeroes between country and area codes
        if row["phone_number"][:len(code)] == code:
            row["phone_number"] = row["phone_number"][len(code):].lstrip("0")
        row["phone_number"] = f"{code}{row['phone_number']}"

        return row
    
    def fix_address_case(self, col):
        # todo: change address to title case
        col = col.str.title()
        return col
    

    def tidy_index(self, df):
        """Drops column generated from old index and resets current index"""

        df.drop(columns="index", inplace=True)
        df.reset_index(drop=True)

        return df