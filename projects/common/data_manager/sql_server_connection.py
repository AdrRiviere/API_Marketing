import logging
import os

import pandas as pd
import pyodbc
from dotenv import load_dotenv


def load_table_from_sql_server(query):
    """
    Connection with SQL Server Database.

    Args:
        query (str): str with the specific query SQL.

    Returns:
        df (dataframe): SQL database in Pandas Format.
    """
    try:
        load_dotenv()  # for python-dotenv method

        config_sql = {
            "UID": os.environ.get("data_hub_uid"),
            "PWD": os.environ.get("data_hub_pwd"),
            "Driver": os.environ.get("data_hub_driver"),
            "Database": os.environ.get("data_hub_database"),
            "Server": os.environ.get("data_hub_server"),
        }

        cnx = pyodbc.connect(**config_sql)
        df = pd.read_sql(query, cnx)
        cnx.close()

    except pyodbc.OperationalError as connection_error:
        logging("SQL Server Connection issue")
        logging(connection_error)

    return df
