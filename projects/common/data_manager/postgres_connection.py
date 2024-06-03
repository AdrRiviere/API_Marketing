import logging
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine


def create_connection_postgres(environment):
    """
    Create Postgres Connection.

    Returns:
        cnx (connection): Connection Postgres.
    """
    try:
        load_dotenv()  # for python-dotenv method

        config_sql = {
            "user": os.environ.get(environment + "_postgres_user"),
            "password": os.environ.get(environment + "_postgres_pass"),
            "host": os.environ.get(environment + "_postgres_host"),
            "database": os.environ.get(environment + "_postgres_dbname"),
            "port": os.environ.get(environment + "_postgres_port"),
        }

        cnx = psycopg2.connect(**config_sql)

    except psycopg2.OperationalError as connection_error:
        logging("Postgres Connection issue")
        logging(connection_error)
    return cnx


def close_connection_postgres(cnx):
    """
    Close Postgres Connection.

    Args:
        cnx (connection): Connection Postgres.
    """

    cnx.close()


def load_table_from_postgres(query):
    """
    Load Table From Postgres Database.

    Args:
        query (str): str with the specific query SQL.

    Returns:
        df (dataframe): SQL database in Pandas Format.
    """
    try:
        cnx = create_connection_postgres()
        cursor = cnx.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        df.columns = [desc[0] for desc in cursor.description]
        close_connection_postgres(cnx)

    except psycopg2.OperationalError as connection_error:
        logging("Postgres Connection issue")
        logging(connection_error)

    return df


def export_table_to_postgres(df, name_table, overwrite, environment, query):
    """
    Export Table in Postgres Database.

    Args:
        df (DataFrame): Table to export to Postgres SQL.
        name_table (str): Name of the table.
        overwrite (str): Replace / Append / Upsert.
        environment (str) : Environment to export (Production - Staging - Dev).
        query (str): Query to upsert data in postgres.

    Returns:
        None
    """

    if overwrite in ["append", "replace"]:
        load_dotenv()  # for python-dotenv method

        config_sql = {
            "user": os.environ.get(environment + "_postgres_user"),
            "password": os.environ.get(environment + "_postgres_pass"),
            "host": os.environ.get(environment + "_postgres_host"),
            "database": os.environ.get(environment + "_postgres_dbname"),
            "port": os.environ.get(environment + "_postgres_port"),
        }

        postgres_url = "postgresql://{}:{}@{}:{}/{}".format(
            config_sql["user"],
            config_sql["password"],
            config_sql["host"],
            config_sql["port"],
            config_sql["database"],
        )
        engine_postgres = create_engine(postgres_url)

        df.to_sql(name_table, engine_postgres, if_exists=overwrite, index=False)

    else:
        # Opening Postgres Connection
        cnx = create_connection_postgres(environment=environment)

        # Create Data to insert in Tuple format
        values_tu_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
        execute_values(cnx.cursor(), query, values_tu_insert)
        cnx.commit()

        # Closing Postgres Connection
        close_connection_postgres(cnx)
