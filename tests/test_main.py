import os

from main import process_fhir_bundles


class TestProcessFhirBundles:
    directory = "tests/data/fhir_bundles"
    processed_dir = "tests/data/fhir_bundles/processed"

    def test_process_fhir_bundles_returns_type_none(self, test_db_cursor):
        cursor = test_db_cursor
        assert process_fhir_bundles(cursor, self.directory) is None

    def test_process_fhir_bundles_moves_files_to_processed_dir(self, test_db_cursor):
        cursor = test_db_cursor
        process_fhir_bundles(cursor, self.directory)
        processed_files = os.listdir(self.processed_dir)
        assert len(processed_files) == 5

    def test_process_fhir_bundles_adds_patients_to_patient_table(self, test_db_cursor):
        cursor = test_db_cursor
        process_fhir_bundles(cursor, self.directory)
        cursor.execute("SELECT * FROM patient")
        patients = cursor.fetchall()
        assert len(patients) == 5

    def test_process_fhir_bundles_adds_events_to_resource_tables(self, test_db_cursor):
        cursor = test_db_cursor
        process_fhir_bundles(cursor, self.directory)
        cursor.execute("SELECT COUNT(*) FROM condition")
        conditions = cursor.fetchone()[0]
        assert conditions == 10

    def test_process_fhir_bundles_ignores_duplicates(self, test_db_cursor):
        cursor = test_db_cursor
        process_fhir_bundles(cursor, self.directory)
        cursor.execute("SELECT COUNT(*) FROM condition")
        conditions = cursor.fetchone()[0]
        assert conditions == 10
        # processing the same files again should not add any new entries
        process_fhir_bundles(cursor, self.directory)
        cursor.execute("SELECT COUNT(*) FROM condition")
        conditions = cursor.fetchone()[0]
        assert conditions == 10

    def test_process_fhir_bundles_allows_for_relational_sql_queries(self, test_db_cursor):
        cursor = test_db_cursor
        process_fhir_bundles(cursor, self.directory)
        # patient name is in patient table, wheras insurance status is in claim table
        cursor.execute(
            """SELECT p.resource#>>'{name,0,given,0}'
            FROM patient p 
            JOIN claim c ON c.patient_id = p.pid
            WHERE c.resource->'insurance'->0->'coverage'->>'display' = 'NO_INSURANCE'
            """
        )
        patients = cursor.fetchall()
        assert len(patients) == 1
        name = patients[0][0]
        assert name == "Aaron697"
