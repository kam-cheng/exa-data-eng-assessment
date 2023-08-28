import json
from json.decoder import JSONDecodeError
import logging
import re

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def parse_json_file(filepath: str) -> dict:
    """Opens a JSON file and converts the data into a Python object, so that
    it can be processed.

    Args:
        filepath (str): The file path of the JSON file to be parsed.

    Returns:
        dict: The parsed JSON data. Will be type dict as this is the only type
        for FHIR files.

    Raises:
        FileNotFoundError: If the file does not exist.
        JSONDecodeError: If the file is not valid JSON.
    """
    try:
        with open(filepath, 'r') as f:
            parsed_data = json.load(f)
            logger.debug(f"Successfully parsed JSON file: {filepath}")
            logger.debug(f"Parsed Data: {parsed_data}")
            return parsed_data

    except JSONDecodeError as e:
        logger.error(f"Invalid JSON file: {filepath}")
        raise e


def retrieve_patient_entry_index(fhir_entries: list) -> int:
    """Retrieves the index of the entry containing resource type Patient from
    the FHIR object key ["entry"] list. Each FHIR file should contain one
    Patient entry, which is necessary so that we can create relationships
    with the remaining entry data.

    Args:
        fhir_entries (list): List of entry objects from the FHIR data.

    Returns:
        int: The list index of the Patient entry.

    Raises:
        PatientNotFoundError: If the Patient resource is not found in the
        entry list.
    """
    for index, entry in enumerate(fhir_entries):
        resource = entry['resource']
        if resource['resourceType'] == 'Patient':
            logger.debug(f"Found patient resource at index {index}")
            print(entry)
            return index

    raise PatientNotFoundError()


class PatientNotFoundError(Exception):
    # Custom exception to be raised when the Patient resource is not found
    def __init__(self):
        self.message = "Unable to locate Patient entry in input entry list."
        super().__init__(self.message)


def pascal_to_snake_case(camel_str: str) -> str:
    """Converts a PascalCase string to snake_case. Used to convert FHIR
    resource type names which are in PascalCase so that we use them for
    psql table naming, which by convention uses snake case."""

    """ Regex explanation:
    1. ([a-z0-9]) matches the first lowercase letter or digit
    2. ([A-Z]) matches the second uppercase letter
    3. \1 and \2 are the first and second matches
    4. r'\1_\2' inserts an underscore between the matches """
    snake_str = re.sub('([a-z0-9])([A-Z])', r'\1_\2', camel_str)
    return snake_str.lower()
