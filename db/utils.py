from psycopg2 import sql, errors
from psycopg2.extensions import cursor

import logging

logger = logging.getLogger(__name__)

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}


def create_database(db_name: str, cursor: cursor):
    """Creates a database with the input name if one doesn't already exist.

    Args:
        db_name (str): name of database to create
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands
    """
    try:
        # Attempt to create the database
        create_db_sql = sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(db_name))  # using sql.Identifier to prevent SQL injection
        cursor.execute(create_db_sql)
        logger.info(f"Database '{db_name}' created successfully.")

    except errors.DuplicateDatabase:
        logger.info(f"Database '{db_name}' already exists.")


def delete_database(db_name: str, cursor: cursor):
    """Deletes a database with the input name if one exists.

    Args:
        db_name (str): name of database to delete
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands
    """

    delete_db_sql = sql.SQL("DROP DATABASE IF EXISTS {}").format(
        sql.Identifier(db_name))  # using sql.Identifier to prevent SQL injection
    cursor.execute(delete_db_sql)
    logger.info(f"Database '{db_name}' deleted successfully.")


def create_patient_table(cursor: cursor):

    create_patient_table_sql = """
            CREATE TABLE IF NOT EXISTS patient (
                pid serial PRIMARY KEY,
                full_url text,
                resource jsonb,
                request jsonb
            )
        """

    cursor.execute(create_patient_table_sql)
    logger.info("Table 'patient' created successfully.")
