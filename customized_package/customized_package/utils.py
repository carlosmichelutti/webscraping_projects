from sqlalchemy import create_engine
from typing import Literal
import mysql.connector
import pandas as pd

class Database:

    def __init__(
        self: object,
        db_user: str,
        db_pass: str,
        db_host: str,
        db_port: str,
        db_name: str
    ) -> None:

        self.engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

        self.empresa_db = {
            'host': db_host,
            'port': db_port,
            'user': db_user,
            'password': db_pass,
            'database': db_name
        }

        self.connection = mysql.connector.connect(**self.empresa_db)
        self.cursor = self.connection.cursor()

    def execute_query_to_clean_database(self: object, query: str) -> None:

        """
            Function to execute a query to clean database.

            Parameters
            ----------
                query: str -> query to be executed.
        """

        self.cursor.execute(query)
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def insert_data(self: object, data: pd.DataFrame, table_name: str, if_exists: Literal['append', 'replace'] | None = ..., index: bool = False) -> None:

        """
            Function to insert data into database.

            Parameters
            ----------
                data: pd.DataFrame -> data to be inserted;
                table_name: str -> name for table to insert data;
                if_exists: Literal['append', 'replace'] | None = ... -> how to handle if table already exists.
                index: bool = False -> if index should be inserted.
        """

        data.to_sql(
            name=table_name, 
            con=self.engine, 
            if_exists=if_exists, 
            index=index
        )

        print('Data entered successfully!')

    def read_query(self: object, query: str) -> pd.DataFrame:

        """
            Function to read data from the database based on a query.

            Parameters
            ----------
                query: str -> query to be executed.

            Returns
            -------
                pd.DataFrame -> data read from database.
        """

        return pd.read_sql(
            query, self.engine
        )
