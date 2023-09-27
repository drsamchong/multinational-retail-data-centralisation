import pandas as pd
import numpy as np

class DataCleaning():
    """ Contains methods to clean data from a variety of data sources """
    
    def clean_user_data(self, user_df):

        # Missing values appear to be represented as a string "NULL" rather than None or np.nan, so replacing
        clean_df = self.replace_null_string(user_df)

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



    def replace_null_string(self, df):
        """Replaces missing values represented as a "NULL" string by np.nan"""

        return df.replace("NULL", np.nan)


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
    
    def check_card_data_structure(self, pdf_dfs):

        expected_index = pdf_dfs[0].columns.tolist()
        odd_pages = {}

        for idx, table in enumerate(pdf_dfs):
            if not all(col_name in table.columns.tolist() for col_name in expected_index):
                # print(f"Page: {idx}")
                # print(table.columns)
                #display(table)
                odd_pages[idx] = table.columns.tolist()

        return odd_pages

    def correct_card_df(self, df, col_names):
        # todo:

        df[["card_number", "expiry_date"]] = df["card_number expiry_date"].str.split(expand=True)
        df.drop("card_number expiry_date", axis=1, inplace=True)
        df = df[col_names]

        return df


    def merge_card_dfs(self, pdf_dfs):
        """Merges dfs from separate pages to single df"""

        merged_df = pdf_dfs[0]

        for table in pdf_dfs[1:]:
            merged_df = pd.concat([merged_df, table], ignore_index=True)

        merged_df.info()
        return merged_df

    def get_non_numeric_card_numbers(self, card_df):
        """Identify which rows contain non-numeric card number values"""

        numeric_card_no = pd.to_numeric(card_df["card_number"], errors="coerce")
        rows_to_clean = numeric_card_no.isna()
        return rows_to_clean
    
    def remove_card_number_prefix(self, card_df, rows):
        """Remove leading '?' characters from card number"""

        
        card_df.loc[rows, "card_number"] = card_df.loc[rows, "card_number"].str.lstrip('?-')
        return card_df
    
    def clean_card_numbers(self, card_df):

        all_non_num_card_no = self.get_non_numeric_card_numbers(card_df)
        card_df = self.remove_card_number_prefix(card_df, all_non_num_card_no)

        # remove remaining non-numeric card numbers
        card_df = card_df[self.get_non_numeric_card_numbers(card_df)]



        pass
    
    def clean_card_data(self, card_data):
        """Performs various stages in cleaning card transaction data extracted from PDF"""

        print(card_data.info())

        # data from tabula consists of arrray of df, some of which may be have an incorrect structure
        malformed_card_dfs = self.check_card_data_structure(card_data)

        if malformed_card_dfs:
            exp_col_names = card_data[0].columns.tolist()
            for pg in malformed_card_dfs.keys():
                card_data[pg] = self.correct_card_df(card_data[pg], exp_col_names)


        # get single df from corrected dfs from each PDF page        
        clean_card_df = self.merge_card_dfs(card_data)

        # NULL to np.nan
        clean_card_df = self.replace_null_string(clean_card_df)

        # drop rows where all values are na
        clean_card_df = clean_card_df.dropna(axis=0, how="all")

        # todo: check providers

        # todo: remove row where unique provider

        # todo: clean card numbers - get non-numeric and remove extra characters

        # todo : check card numbers are correct length


        # todo: provider to category

        # todo: expiry to date

        # todo: payment date to date

        print(clean_card_df.info())
        return clean_card_df


        pass