import yaml
import psycopg2
from sqlalchemy import create_engine, inspect

class DatabaseConnector():


    def __init__(self, creds_file="db_creds.yaml"):
        self.creds_file = creds_file

    def read_db_creds(self):
        with open(self.creds_file, mode="r") as creds:
            db_creds = yaml.safe_load(creds)
#            print(db_creds)
            return db_creds

    def init_db_engine(self, db_creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = db_creds["RDS_USER"]
        PASSWORD = db_creds["RDS_PASSWORD"]
        ENDPOINT = db_creds["RDS_HOST"]
        PORT = db_creds["RDS_PORT"]
        DATABASE = db_creds["RDS_DATABASE"]
        url = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
        engine = create_engine(url)
        # TODO: read db_creds and initialise and return a sqlalchemy db engine
        return engine

    def list_db_tables(self, db_engine):
        # SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
        inspector = inspect(db_engine)
        tables = inspector.get_table_names()
        print("Tables in DB:")
        for table in tables:
            print(table) 
        


if __name__ == "__main__":
    conn = DatabaseConnector()
    db_creds = conn.read_db_creds()
#    print(db_creds)
    engine = conn.init_db_engine(db_creds)
    #engine.connect()
    conn.list_db_tables(engine)
