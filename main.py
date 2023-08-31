import psycopg2
from psycopg2 import extensions

import os
import logging
import shutil

from db.utils import create_patient_table, create_gin_index
from process.process import process_fhir_bundle
from process.utils import retrieve_json_filenames


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def process_fhir_bundles(cursor: extensions.cursor, directory: str) -> None:
    """Process all FHIR bundles in a directory, adding it to the database,
    and moving the processed files to a processed directory.

    Args:
        cursor (extensions.cursor): psycopg2 cursor object
        directory (str): directory containing FHIR bundles
    """

    create_patient_table(cursor)

    fhir_files = retrieve_json_filenames(directory)
    logger.info(f"list of FHIR files found in {directory}: {fhir_files}")

    tables = set()

    counter = 0
    for file in fhir_files:

        file_path = os.path.join(directory, file)
        logger.info(f"Processing FHIR bundle: {file_path}")
        tables = process_fhir_bundle(file_path, cursor, tables)
        counter += 1

        # move processed file to processed directory so it is not reprocessed
        processed_path = os.path.join(directory, "processed", file)
        shutil.move(file_path, processed_path)

    # create GIN index on resource column for each table for faster querying
    for table in tables:
        create_gin_index(cursor, table, "resource")
    logging.info(f"Indexing Completed for {len(tables)} tables.")
    logging.info(f"Finished processing {counter} FHIR bundles!")


if __name__ == "__main__":
    db_params = {
        "dbname": "dev_patient_db",
        "user": "postgres",
        "password": "password",
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": 5432,
    }

    connection = psycopg2.connect(**db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    directory = "data"
    process_fhir_bundles(cursor, directory)
    cursor.close()
    connection.close()
