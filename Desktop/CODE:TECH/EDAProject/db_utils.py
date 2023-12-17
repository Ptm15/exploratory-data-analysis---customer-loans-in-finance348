

# Creating a class called RDSDatabaseConnector - containing methods which will be used to extract data from  the RDS Database 

from sqlalchemy import create_engine
import pandas as pd

class RDSDatabaseConnector:
    def __init__(self, username, password, host, database_name):
        self.username = username
        self.password = password
        self.host = host
        self.database_name = database_name
        self.engine = self._create_engine()

    def _create_engine(self):
        # Create a connection string
        connection_str = f"postgresql://{self.username}:{self.password}@{self.host}/{self.database_name}"
        # Create an engine
        engine = create_engine(connection_str)
        return engine

    def extract_data(self, query):
        # Execute the query and return data as a DataFrame
        with self.engine.connect() as connection:
            result = connection.execute(query)
            data = pd.DataFrame(result.fetchall(), columns=result.keys())
            return data


