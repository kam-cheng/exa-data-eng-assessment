from psycopg2 import extensions
from process.utils import (
    parse_json_file, retrieve_patient_entry_index,
    pascal_to_snake_case)
from db.utils import add_patient_entry, add_event_entry, create_resource_table


def process_fhir_bundle(filepath: str, cursor: extensions.cursor,
                        tables: list = []):
    """Retrieves the FHIR bundle from the specified filepath and processes it.

    Processing involves:
    - Parsing the FHIR .json file to retrieve the data
    - Creating a patient entry in the patient table
    - Creating a resource table for each new resource type in the FHIR bundle
    - Adding an event entry for each resource in the FHIR bundle

    Args:
        filepath (str): filepath of the FHIR bundle to be processed
        cursor (extensions.cursor): psycopg2 cursor object for querying the database

    Returns:
        tables(list): updated list of table names in the database
    """
    # copy the table to avoid mutating the original
    tables = tables.copy()
    # convert json to python object so we can access the data
    parsed_data = parse_json_file(filepath)
    # entries are stored in a list
    entries: list = parsed_data["entry"]
    # retrieve the patient entry so we can add it to the patient table
    patient_entry = entries[retrieve_patient_entry_index(entries)]
    # patient id is used to link other events to the patient
    patient_id: int = add_patient_entry(cursor, patient_entry)
    tables.append("patient")

    for entry in entries:
        resource_type = entry['resource']['resourceType']
        # skip patient entry as we have already added it to the patient table
        if resource_type == 'Patient':
            continue
        # Each resource type should have its own table, but we need to
        # convert the value to snake case to make a conventional table name.
        table_name = pascal_to_snake_case(resource_type)
        # Only create a new table if it does not already exist
        if table_name not in tables:
            create_resource_table(cursor, table_name)
            tables.append(table_name)
        # Finally, add the event entry to the table with a matching resource
        # type.
        add_event_entry(cursor, table_name, entry, patient_id)

    # return the updated list of tables
    return tables
