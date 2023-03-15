import yaml

class DatabaseConnector():


    def __init__(self, creds_file="db_creds.yaml"):
        self.creds_file = creds_file

    def read_db_creds(self):
        with open(self.creds_file, mode="r") as creds:
            db_creds = yaml.safe_load(creds)
            print(db_creds)



# conn = DatabaseConnector()
# conn.read_db_creds()