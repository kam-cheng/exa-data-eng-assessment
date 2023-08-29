from psycopg2.extensions import cursor
from psycopg2 import sql

import pytest

from process.process import process_fhir_bundle
from db.utils import create_patient_table


@pytest.fixture
def db_cursor(test_db_cursor) -> cursor:
    create_patient_table(test_db_cursor)
    yield test_db_cursor


class TestProcessFhirBundle:
    fhir_filepath = "tests/data/fhir_02.json"
    """
    fhir_02.json contains a truncated version of the FHIR bundle.
    The bundle contains 6 entries:
    - 1 Patient
    - 1 Encounter
    - 2 Condition
    - 1 DiagnosticReport
    - 1 DocumentReference
    """
    expected_tables = ["patient", "encounter", "condition",
                       "diagnostic_report", "document_reference"]

    def test_process_fhir_bundle_returns_type_list(self, db_cursor):
        cursor = db_cursor
        assert isinstance(process_fhir_bundle(
            self.fhir_filepath, cursor), list)

    def test_process_fhir_bundle_returns_list_of_correct_length(self, db_cursor):
        cursor = db_cursor
        tables = process_fhir_bundle(self.fhir_filepath, cursor)
        assert len(tables) == len(self.expected_tables)

    def test_process_fhir_bundle_returns_list_of_correct_table_names(self, db_cursor):
        cursor = db_cursor
        tables = process_fhir_bundle(self.fhir_filepath, cursor)
        # tables should be using snake case
        for expected_table in self.expected_tables:
            assert expected_table in tables

    def test_process_fhir_bundle_creates_tables_in_database(self, db_cursor):
        cursor = db_cursor
        process_fhir_bundle(self.fhir_filepath, cursor)
        table_names_sql = sql.SQL("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        cursor.execute(table_names_sql)
        table_names = cursor.fetchall()
        table_names = [name[0] for name in table_names]
        assert len(table_names) == len(self.expected_tables)
        for expected_table in self.expected_tables:
            assert expected_table in table_names

    def test_process_fhir_bundle_adds_correct_number_of_entries_to_each_table(self, db_cursor):
        cursor = db_cursor
        process_fhir_bundle(self.fhir_filepath, cursor)
        for table in self.expected_tables:
            cursor.execute(sql.SQL(f"SELECT * FROM {table}"))
            entries = len(cursor.fetchall())
            if table == "condition":
                assert entries == 2
            else:
                assert entries == 1

    def test_process_fhir_bundle_db_can_be_queried_using_jsonb(self, db_cursor):
        cursor = db_cursor
        process_fhir_bundle(self.fhir_filepath, cursor)
        # resource stores data in jsonb format
        birthdate_sql = sql.SQL("""
             SELECT resource -> 'birthDate'
            FROM patient
            WHERE pid = '1'
        """)
        cursor.execute(birthdate_sql)
        birth_date = cursor.fetchone()[0]
        assert birth_date == "1944-08-28"

    def test_process_fhir_bundle_db_can_query_relational_data(self, db_cursor):
        cursor = db_cursor
        process_fhir_bundle(self.fhir_filepath, cursor)
        relational_sql = """
            SELECT
                patient.resource->'address'->0->>'city',
                condition.resource->'id'
            FROM
                patient
            JOIN
                condition
            ON patient.pid = condition.patient_id
            WHERE condition.resource->'code'->'coding'->0->>'code' = '713458007'

        """
        cursor.execute(relational_sql)
        city, id = cursor.fetchone()
        assert city == "Charlton"
        assert id == "b6487994-9b3f-7b3c-6e2d-feba134129f9"
