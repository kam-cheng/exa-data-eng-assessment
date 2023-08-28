import psycopg2
from psycopg2 import sql, errors
from psycopg2.extensions import cursor

import pytest
import logging

from db.utils import (create_database, delete_database,
                      create_patient_table, add_patient_entry,
                      db_params)
from tests.data.patient_entry import PATIENT_ENTRY

test_db_params = {
    "dbname": "test_database_fhir",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}


@pytest.fixture
def db_cursor() -> cursor:
    """Pytest fixture which does the following:
    1. Connects to the default database
    2. Drops the test database if it already exists
    3. Yields a cursor object
    4. Closes the cursor and connection"""
    connection = psycopg2.connect(**db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(
        sql.SQL("DROP DATABASE IF EXISTS {}").format(
            sql.Identifier(TestCreateDatabase.test_db_name)))
    yield cursor
    cursor.close()
    connection.close()


@pytest.fixture
def create_test_db():
    """Pytest fixture which creates a test database if it does not already exist"""
    try:
        connection = psycopg2.connect(**db_params)
        connection.autocommit = True
        cursor = connection.cursor()
        create_db_sql = sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(TestCreateDatabase.test_db_name))
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
    yield test_db_cursor


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


class TestCreatePatientTable:
    patient_table_columns = ['pid', 'full_url', 'resource', 'request']
    patient_table_name_data_type = [
        ('pid', 'integer'), ('resource', 'jsonb'),
        ('request', 'jsonb'), ('full_url', 'text')]

    def test_create_patient_table_returns_type_none(self, test_db_cursor):
        assert create_patient_table(test_db_cursor) is None

    def test_create_patient_table_contains_the_correct_column_names(
            self, test_db_cursor):
        create_patient_table(test_db_cursor)
        test_db_cursor.execute(
            """SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'patient'""")
        columns = [column[0] for column in test_db_cursor.fetchall()]
        assert sorted(columns) == sorted(self.patient_table_columns)

    def test_create_patient_table_columns_contain_correct_data_types(self, test_db_cursor):
        create_patient_table(test_db_cursor)
        test_db_cursor.execute(
            """SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'patient'""")
        column_name_and_data_types = test_db_cursor.fetchall()
        assert column_name_and_data_types == self.patient_table_name_data_type


class TestAddPatientEntry:
    full_url = "test_url"
    resource = {"resourceType": "Patient"}
    request = {"method": "POST", "url": "Patient"}
    test_data = {
        "fullUrl": full_url,
        "resource": resource,
        "request": request
    }

    def test_add_patient_entry_returns_type_int(self, test_create_table):
        cursor = test_create_table
        assert isinstance(add_patient_entry(
            cursor, PATIENT_ENTRY), int)

    def test_add_patient_entry_returns_correct_value(self, test_create_table):
        cursor = test_create_table
        pid = add_patient_entry(cursor, self.test_data)
        assert pid == 1

    def test_add_patient_entry_adds_entry_to_patient_table(self, test_create_table):
        cursor = test_create_table
        add_patient_entry(cursor, self.test_data)
        cursor.execute(
            """SELECT * FROM patient""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 1

    def test_add_patient_entry_adds_correct_data_to_patient_table(self, test_create_table):
        add_patient_entry(test_create_table, self.test_data)
        cursor = test_create_table
        cursor.execute(
            """SELECT * FROM patient""")
        pid, full_url, resource, request = cursor.fetchone()
        assert pid == 1
        assert full_url == self.full_url
        assert resource == self.resource
        assert request == self.request

    def test_add_patient_entry_increments_pid_for_each_entry(self, test_create_table):
        cursor = test_create_table
        add_patient_entry(cursor, self.test_data)
        second_patient = self.test_data.copy()
        # fullUrl is unique so we need to change it
        second_patient["fullUrl"] = "second_url"
        add_patient_entry(cursor, second_patient)
        cursor.execute(
            """SELECT * FROM patient""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 2
        # index 0 contains the pid
        assert patient_entries[0][0] == 1
        assert patient_entries[1][0] == 2

    def test_add_patient_entry_rejects_duplicate_full_url(self, test_create_table):
        # Since the return value is always an int, a -1 will indicate that the
        # entry was not added
        cursor = test_create_table
        pid = add_patient_entry(cursor, self.test_data)
        assert pid == 1
        # Attempt to add the same patient entry again
        pid = add_patient_entry(cursor, self.test_data)
        assert pid == -1
        # confirm only one entry was added
        cursor.execute(
            """SELECT * FROM patient""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 1

    def test_add_patient_entry_logs_error_if_duplicate_full_url(self, test_create_table, caplog):
        cursor = test_create_table
        add_patient_entry(cursor, self.test_data)
        # Attempt to add the same patient entry again
        add_patient_entry(cursor, self.test_data)
        assert f"Patient entry with fullUrl '{self.full_url}' already exists." in caplog.text

    def test_add_patient_entry_succeeds_for_sample_patient_entry_data(self, test_create_table):
        # test using the sample patient entry data to ensure it works as intended.
        cursor = test_create_table
        pid = add_patient_entry(cursor, PATIENT_ENTRY)
        assert pid == 1
        cursor.execute(
            """SELECT * FROM patient""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 1
        (pid, full_url, resource, request) = patient_entries[0]
        assert full_url == PATIENT_ENTRY["fullUrl"]
        assert resource == PATIENT_ENTRY["resource"]
        assert request == PATIENT_ENTRY["request"]
