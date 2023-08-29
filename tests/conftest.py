"""Store for pytest fixtures shared across files"""
import psycopg2
from psycopg2 import sql, errors
from psycopg2.extensions import cursor
from db.utils import create_patient_table, create_resource_table

import pytest

from db.utils import db_params

test_db_params = {
    "dbname": "test_patient_db",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}


@pytest.fixture
def create_test_db():
    """Pytest fixture which creates a test database if it does not already exist"""
    try:
        connection = psycopg2.connect(**db_params)
        connection.autocommit = True
        cursor = connection.cursor()
        create_db_sql = sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier("test_db"))
        cursor.execute(create_db_sql)
        cursor.close()
        connection.close()
    except errors.DuplicateDatabase:
        pass


@pytest.fixture
def clear_tables(create_test_db):
    """Pytest fixture which clears all tables in the test database before each test"""
    connection = psycopg2.connect(**test_db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    get_tables_query = """SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'"""
    cursor.execute(get_tables_query)
    table_list = [table[0] for table in cursor.fetchall()]
    for table in table_list:
        cursor.execute(sql.SQL(f"DROP TABLE {table} CASCADE"))
    cursor.close()
    connection.close()


@pytest.fixture
def test_db_cursor(clear_tables) -> cursor:
    # Pytest fixture which yields a cursor object connected to the test database
    connection = psycopg2.connect(**test_db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    yield cursor
    cursor.close()
    connection.close()


@pytest.fixture
def test_create_table(test_db_cursor) -> cursor:
    # Creates tables and returns a cursor object connected to the test database
    create_patient_table(test_db_cursor)
    create_resource_table(test_db_cursor, "resource_type_01")
    yield test_db_cursor
