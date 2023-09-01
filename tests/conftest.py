"""Store for pytest fixtures shared across files"""
import psycopg2
from psycopg2 import sql, errors
from psycopg2.extensions import cursor
import pytest

import os
import shutil

from db.utils import create_patient_table, create_resource_table
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
def test_db_cursor(clear_tables, setup_fhir_dir) -> cursor:
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


@pytest.fixture()
def setup_fhir_dir():
    source_directory = "tests/data/fhir_bundles/processed"
    destination_directory = "tests/data/fhir_bundles"
    # Retrieve the list of files in the source directory
    files_to_move = os.listdir(source_directory)
    # Move files from the source to the destination directory
    for filename in files_to_move:
        source_path = os.path.join(source_directory, filename)
        destination_path = os.path.join(destination_directory, filename)
        shutil.move(source_path, destination_path)
    yield  # Yield to the test function
