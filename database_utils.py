import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector():


    def __init__(self, creds_file="db_creds.yaml"):
        self.creds_file = creds_file

    def read_db_creds(self):
        with open(self.creds_file, mode="r") as creds:
            db_creds = yaml.safe_load(creds)
#            print(db_creds)
            return db_creds

    def init_db_engine(self):
        """Read db creadential file and returns a SQLalchemy connection engine"""

        db_creds = self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = db_creds["RDS_USER"]
        PASSWORD = db_creds["RDS_PASSWORD"]
        ENDPOINT = db_creds["RDS_HOST"]
        PORT = db_creds["RDS_PORT"]
        DATABASE = db_creds["RDS_DATABASE"]
        url = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
        engine = create_engine(url)
        return engine

    def list_db_tables(self, db_engine):
        """Print the tables existing in the database"""

        # SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
        inspector = inspect(db_engine)
        tables = inspector.get_table_names()
        print("Tables in DB:\n")
        for table in tables:
            print(table) 
        

    def upload_to_db(self, df, name):
        """Create database engine and write dataframe to database table with specified name"""
        
        engine = self.init_db_engine()
        df.to_sql(name, engine, if_exists="replace")



if __name__ == "__main__":
    conn = DatabaseConnector()
    db_creds = conn.read_db_creds()
    engine = conn.init_db_engine(db_creds)
    #engine.connect()
    conn.list_db_tables(engine)
