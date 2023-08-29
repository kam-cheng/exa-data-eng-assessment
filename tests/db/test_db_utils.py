import psycopg2
from psycopg2 import sql
from psycopg2.extensions import cursor

import pytest
import logging

from db.utils import (create_database, delete_database,
                      create_patient_table, add_patient_entry,
                      create_resource_table, add_event_entry,
                      retrieve_table_names,
                      db_params)
from tests.data.patient_entry import PATIENT_ENTRY
from tests.data.encounter_entry import ENCOUNTER_ENTRY


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
def add_patients(test_create_table):
    # add 5 patients to the patient table
    cursor = test_create_table
    patients = [{"fullUrl": f"urn:uuid:Patient-{i}", "resource": {
        "name": f"Patient {i}"}, "request": {"method": "POST"}} for i in range(1, 6)]
    for patient in patients:
        add_patient_entry(cursor, patient)
    yield cursor


class TestCreateDatabase:
    test_db_name = "test_db"

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
    test_db_name = "test_db"

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
        # iterate over list as we cannot guarantee the order of the columns
        for name_data in self.patient_table_name_data_type:
            assert name_data in column_name_and_data_types


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


class TestCreateResourceTable:
    resource_table_columns = ['id', 'full_url',
                              'request', 'resource', 'patient_id']
    resource_table_name_data_type = [('id', 'integer'), ('request', 'jsonb'), (
        'resource', 'jsonb'), ('patient_id', 'integer'), ('full_url', 'text')]
    table_name = "test_resource"

    def test_create_resource_table_returns_type_none(self, test_create_table):
        cursor = test_create_table
        assert create_resource_table(cursor, self.table_name) is None

    def test_create_resource_table_creates_table_with_correct_name(self, test_create_table):
        cursor = test_create_table
        create_resource_table(cursor, self.table_name)
        cursor.execute(
            """SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'""")
        tables = [table[0] for table in cursor.fetchall()]
        assert self.table_name in tables

    def test_create_resource_table_contains_the_correct_column_names(
            self, test_create_table):
        cursor = test_create_table
        create_resource_table(cursor, self.table_name)
        cursor.execute(
            """SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'test_resource'""")
        columns = [column[0] for column in cursor.fetchall()]
        assert sorted(columns) == sorted(self.resource_table_columns)

    def test_create_resource_table_columns_contain_correct_data_types(self, test_create_table):
        cursor = test_create_table
        create_resource_table(cursor, self.table_name)
        cursor.execute(
            """SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_resource'""")
        column_name_and_data_types = cursor.fetchall()
        # iterate over list as we cannot guarantee the order of the columns
        for name_data in self.resource_table_name_data_type:
            assert name_data in column_name_and_data_types

    def test_create_resource_table_can_create_multiple_tables(self, test_create_table):
        cursor = test_create_table
        tables = ["first_resource_table",
                  "second_resource_table", "third_resource_table"]
        cursor.execute(
            """SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'""")
        tables_before = len(cursor.fetchall())

        for table in tables:
            create_resource_table(cursor, table)
        cursor.execute(
            """SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'""")
        tables = cursor.fetchall()
        assert len(tables) - tables_before == 3


class TestAddEventEntry:
    table_name = "resource_type_01"
    full_url = "resource_01_url"
    resource = {"resourceType": "resource 01"}
    request = {"method": "POST", "url": "resource_01"}
    test_data = {
        "fullUrl": full_url,
        "resource": resource,
        "request": request
    }
    patient_id = 1

    def test_add_event_entry_returns_type_int(self, add_patients):
        cursor = add_patients
        assert isinstance(add_event_entry(
            cursor, self.table_name, self.test_data, self.patient_id), int)

    def test_add_event_entry_returns_correct_value(self, add_patients):
        cursor = add_patients
        id = add_event_entry(cursor, self.table_name,
                             self.test_data, self.patient_id)
        assert id == 1
        new_entry = self.test_data.copy()
        # add another entry to check that id increments
        new_entry["fullUrl"] = "new_event_url"
        id = add_event_entry(cursor, self.table_name,
                             new_entry, self.patient_id)
        assert id == 2

    def test_add_event_entry_adds_entry_to_resource_table(self, add_patients):
        cursor = add_patients
        # check table is empty first
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        resource_entries = cursor.fetchall()
        assert len(resource_entries) == 0
        add_event_entry(cursor, self.table_name,
                        self.test_data, self.patient_id)
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        resource_entries = cursor.fetchall()
        assert len(resource_entries) == 1

    def test_add_event_entry_adds_correct_data_to_resource_table(self, add_patients):
        cursor = add_patients
        add_event_entry(cursor, self.table_name,
                        self.test_data, self.patient_id)
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        id, full_url, request, resource, patient_id = cursor.fetchone()
        assert id == 1
        assert full_url == self.full_url
        assert resource == self.resource
        assert request == self.request
        assert patient_id == self.patient_id

    def test_add_event_entry_rejects_duplicate_entries(self, add_patients):
        cursor = add_patients
        add_event_entry(cursor, self.table_name,
                        self.test_data, self.patient_id)
        id = add_event_entry(cursor, self.table_name,
                             self.test_data, self.patient_id)
        assert id == -1
        # confirm only one entry was added
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 1

    def test_add_event_entry_logs_error_if_duplicate_full_url(self, add_patients, caplog):
        cursor = add_patients
        add_event_entry(cursor, self.table_name,
                        self.test_data, self.patient_id)
        # Attempt to add the same patient entry again
        add_event_entry(cursor, self.table_name,
                        self.test_data, self.patient_id)
        assert f"Resource entry with fullUrl '{self.full_url}' already exists." in caplog.text

    def test_add_event_entry_rejects_request_if_patient_id_does_not_exist(self, add_patients):
        cursor = add_patients
        patient_id = 100
        id = add_event_entry(cursor, self.table_name,
                             self.test_data, patient_id)
        assert id == -1  # minus value indicates error
        # confirm only one entry was added
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        patient_entries = cursor.fetchall()
        assert len(patient_entries) == 0

    def test_add_event_entry_logs_error_if_patient_id_does_not_exist(self, add_patients, caplog):
        cursor = add_patients
        patient_id = 100
        add_event_entry(cursor, self.table_name,
                        self.test_data, patient_id)
        assert f"Patient with pid '{patient_id}' does not exist." in caplog.text

    def test_add_event_entry_succeeds_for_sample_event_entry_data(self, add_patients):
        cursor = add_patients
        id = add_event_entry(cursor, self.table_name,
                             ENCOUNTER_ENTRY, self.patient_id)
        assert id == 1
        cursor.execute(
            f"""SELECT * FROM {self.table_name}""")
        resource_entries = cursor.fetchall()
        assert len(resource_entries) == 1
        (id, full_url, request, resource, patient_id) = resource_entries[0]
        assert full_url == ENCOUNTER_ENTRY["fullUrl"]
        assert resource == ENCOUNTER_ENTRY["resource"]
        assert request == ENCOUNTER_ENTRY["request"]
        assert patient_id == self.patient_id


class TestRetrieveTableNames:
    tables = ["patient", "resource_type_01"]

    def test_retrieve_table_names_returns_type_list(self, test_create_table):
        cursor = test_create_table
        assert isinstance(retrieve_table_names(cursor), list)

    def test_retrieve_table_names_returns_correct_number_of_tables(self, test_create_table):
        cursor = test_create_table
        tables = retrieve_table_names(cursor)
        assert len(tables) == 2

    def test_retrieve_table_names_returns_correct_table_names(self, test_create_table):
        cursor = test_create_table
        tables = retrieve_table_names(cursor)
        assert tables == self.tables
