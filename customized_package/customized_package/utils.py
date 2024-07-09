from sqlalchemy import create_engine
from typing import Literal
from time import sleep
import mysql.connector
import pandas as pd
import pyexcel
import os

class Database:

    def __init__(
        self: object,
        db_user: str,
        db_pass: str,
        db_host: str,
        db_port: str,
        db_name: str,
    ) -> None:

        """
            Class initiator for mysql database connection.

            Parameters
            ----------
                user: str -> database user;
                password: str -> database password;
                host: str -> database host;
                port: str -> database port;
                database: str -> database name.
            
            Attributes
            ----------
                self.engine -> database connection engine.
        """

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

    def execute_query_to_clean_database(self: object, query: str):
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            results = self.cursor.fetchall()
            print(f'Query {query} completed successfully. {len(results)} affected rows.')
        except Exception as e:
            print(f'Query {query} invalid, please review. Error {e}.')

        self.cursor.close()
        self.connection.close()

    def insert_data(
        self:object, 
        data:pd.DataFrame, 
        table_name:str, 
        if_exists:Literal['append', 'replace'] | None = ...,
        index:bool=False
    ) -> None:
        
        """
            Function to insert data into database.

            Parameters
            ----------
                data:pd.DataFrame -> data to be inserted;
                table_name:str -> name for table to insert data;
                if_exists: Literal['append', 'replace'] -> how to handle if table already exists.
                index: bool -> if index should be inserted.
        """

        data.to_sql(
            name=table_name, 
            con=self.engine, 
            if_exists=if_exists, 
            index=index
        )

        print('Data entered successfully!')

    def read_query(self: object, query: str):

        """
            Function to read data from database.

            Parameters
            ----------
                query: str -> query to be executed;

            Returns
            -------
                pd.DataFrame -> data read from database.
        """

        try:
            return pd.read_sql(
                query, self.engine
            )

        except Exception as e:
            print(f'Query {query} invalid, please review. Error {e}.')
