"""WARNING: Should only be run once, as it will delete any existing databases
of the same name.

This script will delete and create two new databases:
- test_patient_db (for testing)
- dev_patient_db (for development)
"""
import psycopg2

import logging

from db.utils import db_params, create_database, delete_database

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

connection = psycopg2.connect(**db_params)
connection.autocommit = True
cursor = connection.cursor()

delete_database("dev_patient_db", cursor)
delete_database("test_patient_db", cursor)
create_database("dev_patient_db", cursor)
create_database("test_patient_db", cursor)

connection.close()
cursor.close()
