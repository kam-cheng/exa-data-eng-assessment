from psycopg2 import sql, errors
from psycopg2.extensions import cursor
from psycopg2.extras import Json

import logging

logger = logging.getLogger(__name__)

db_params = {
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
    """Creates a table named 'patient' to store the FHIR patient entry data.

    The created table will contain the following columns:
        pid: unique serial key for each patient that other tables can reference
        full_url: the fullUrl value of the patient entry (must be unique)
        resource: jsonb representation of the patient resource object
        request: jsonb representation of the request object

    Args:
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands

    """

    create_patient_table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS patient (
                pid serial PRIMARY KEY,
                full_url text UNIQUE,
                resource jsonb,
                request jsonb
            )
        """)

    cursor.execute(create_patient_table_sql)
    logger.info("Table 'patient' created successfully.")


def create_resource_table(cursor: cursor, table_name: str):
    """Creates a new table to store FHIR entries of matching resource types.

    The created table will contain the following columns:
        id: unique serial key for each resource entry
        full_url: the fullUrl value of the resource entry (must be unique)
        request: jsonb representation of the request object
        resource: jsonb representation of the resource object
        patient_id: foreign key reference to the patient table pid value

    Args:
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands
        table_name (str): name of the table to create

    """

    resource_table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id serial PRIMARY KEY,
                full_url text UNIQUE,
                request jsonb,
                resource jsonb,
                patient_id integer REFERENCES patient(pid) NOT NULL
            )
        """).format(sql.Identifier(table_name))

    cursor.execute(resource_table_sql)
    logger.info(f"New Table '{table_name}' created successfully.")


def add_patient_entry(cursor: cursor, patient_entry: dict) -> int:
    """Parses the Patient Entry, and adds it the 'patient' table. Creates a
    unique identifier for the patient (pid) which can be referenced elsewhere.

    Args:
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands
        patient_entry (dict): patient entry to add to the database

    Returns:
        int: pid of the patient entry that was added to the database. If the
        patient fullUrl already exists in the database, returns -1 to indicate
        that the patient was not added.
    """

    full_url = patient_entry["fullUrl"]
    # make data suitable for jsonb format
    resource = Json(patient_entry["resource"])
    request = Json(patient_entry["request"])

    insert_patient_sql = sql.SQL("""
        INSERT INTO patient (full_url, resource, request)
        VALUES (%s, %s, %s)
        RETURNING pid
    """)
    try:
        cursor.execute(insert_patient_sql,
                       (full_url, resource,
                        request))
        pid = cursor.fetchone()[0]
        logger.info(
            f"Patient entry added successfully. PID: {pid}")

    except errors.UniqueViolation:
        logger.error(
            f"Patient entry with fullUrl '{full_url}' already exists.")
        pid = -1

    return pid


def add_event_entry(cursor: cursor, table_name: str, event: dict, patient_id: int) -> int:
    """Parses the event entry, and adds it to the corresponding table. The Table 
    should match the resource type of the event entry.

    Args:
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands
        table_name (str): name of the table to add the resource entry to
        event (dict): event entry to add to the database
        patient_id (int): pid of the patient that the resource entry belongs to

    Returns:
        int: id of the resource entry that was added to the database. If the
        resource fullUrl already exists in the database, returns -1 to indicate
        that the resource was not added.
    """

    full_url = event["fullUrl"]
    # make data suitable for jsonb format
    resource = Json(event["resource"])
    request = Json(event["request"])

    insert_resource_sql = sql.SQL("""
        INSERT INTO {} (full_url, resource, request, patient_id)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """).format(sql.Identifier(table_name))

    try:
        cursor.execute(insert_resource_sql,
                       (full_url, resource,
                        request, patient_id))
        id = cursor.fetchone()[0]
        logger.info(
            f"Entry added successfully to {table_name}. ID: {id}")

    except errors.UniqueViolation:
        logger.error(
            f"Resource entry with fullUrl '{full_url}' already exists.")
        id = -1

    except errors.ForeignKeyViolation:
        logger.error(
            f"Patient with pid '{patient_id}' does not exist.")
        id = -1

    return id


def retrieve_table_names(cursor: cursor) -> list:
    """Retrieves a list of table names from the database.

    Args:
        cursor (psycopg2.extensions.cursor): cursor object to execute SQL commands

    Returns:
        list: list of table names
    """

    table_names_sql = sql.SQL("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)

    cursor.execute(table_names_sql)
    table_names = cursor.fetchall()
    table_names = [name[0] for name in table_names]
    logger.info(f"Tables in database: {table_names}")
    return table_names
