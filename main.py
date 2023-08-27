import json
from json.decoder import JSONDecodeError
import logging

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
