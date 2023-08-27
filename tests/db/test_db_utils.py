import psycopg2
from psycopg2.extensions import cursor

import pytest
import logging

from db.utils import create_database, delete_database, db_params


@pytest.fixture
def db_cursor() -> cursor:
    """Pytest fixture which does the following:
    1. Connects to the default database
    2. Drops the test database if it already exists
    3. Yields a cursor object
    4. Closes the cursor and connection"""
    connection = psycopg2.connect(db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(
        f"DROP DATABASE IF EXISTS {TestCreateDatabase.test_db_name}")
    yield cursor
    cursor.close()
    connection.close()


class TestCreateDatabase:
    test_db_name = "test_database_fhir"

    def test_create_database_returns_type_none(self, db_cursor):
        assert create_database(self.test_db_name, db_cursor) is None

    def test_create_database_creates_new_database(self, db_cursor: cursor):
        # check that the database does not already exist
        db_cursor.execute(
            f"SELECT datname FROM pg_database WHERE datname='{self.test_db_name}'")
        assert db_cursor.fetchone() is None
        create_database(self.test_db_name, db_cursor)
        # check that the database now exists
        db_cursor.execute(
            f"SELECT datname FROM pg_database WHERE datname='{self.test_db_name}'")
        assert db_cursor.fetchone()[0] == self.test_db_name

    def test_create_database_logs_message_if_database_already_exists(self, db_cursor: cursor, caplog):
        # create the database
        create_database(self.test_db_name, db_cursor)
        # check that the database already exists
        caplog.set_level(logging.INFO)
        create_database(self.test_db_name, db_cursor)
        assert f"Database '{self.test_db_name}' already exists." in caplog.text


class TestDeleteDatabase:
    test_db_name = "test_database_fhir"

    def test_delete_database_returns_type_none(self, db_cursor):
        assert delete_database(self.test_db_name, db_cursor) is None

    def test_delete_database_deletes_existing_database(self, db_cursor: cursor):
        # create the database
        create_database(self.test_db_name, db_cursor)
        # check that the database already exists
        db_cursor.execute(
            f"SELECT datname FROM pg_database WHERE datname='{self.test_db_name}'")
        assert db_cursor.fetchone()[0] == self.test_db_name
        delete_database(self.test_db_name, db_cursor)
        # check that the database now exists
        db_cursor.execute(
            f"SELECT datname FROM pg_database WHERE datname='{self.test_db_name}'")
        assert db_cursor.fetchone() is None
