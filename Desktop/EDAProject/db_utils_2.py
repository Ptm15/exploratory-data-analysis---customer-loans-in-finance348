
# Importing the necessary modules 

import yaml
from sqlalchemy import create_engine
import pandas as pd

# RDSSatabaseConnector class created before in db_utils.py file 

class RDSDatabaseConnector:
    def __init__(self, credentials_file):
        self.credentials = self.load_credentials(credentials_file)
        self.engine = self._create_engine()

    
    # This function loads the credentials that are stored in my .yaml file 

    def load_credentials(self, file_path):
        with open(file_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def _create_engine(self):
        connection_str = f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
        engine = create_engine(connection_str)
        return engine
    
    # function which will alow extraction of data from the table in my DSR database 

    def extract_data_from_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
        return data
    
    # Lods the Dad to .csv file 

    def save_to_csv(self, data, file_path):
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)

    def load_data_to_dataframe(self, table_name):
        query = f"SELECT * FROM {table_name}"
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                data = pd.DataFrame(result.fetchall())
                data.columns = result.keys()
                return data
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_table_names(self, schema='public'):
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                table_names = [row[0] for row in result.fetchall()]
                return table_names
        except Exception as e:
            print(f"An error occurred: {e}")
            return None


connector = RDSDatabaseConnector('/Users/ptm/Desktop/EDAProject/credentials.yaml')

# Getting the table names from the RDS database connector so that I can then view the table using SQL below
tables = connector.get_table_names()

if tables is not None:
    print("Table names:")
    for table in tables:
        print(table)
else:
    print("Unable to retrieve table names. Check your schema or database connectivity.")


table_name = 'loan_payments'

# Load data from the 'loan_payments' table into a pandas df 
data = connector.load_data_to_dataframe(table_name)

# Displaying the first few rows of the loan_payment df 

if data is not None:
    print("Data loaded successfully!")
    print(data.head())  
else:
    print("Unable to load data. Please check the table name or database connectivity.")

