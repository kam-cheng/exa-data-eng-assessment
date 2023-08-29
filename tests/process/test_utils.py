import pytest
import json
import os

from process.utils import (
    parse_json_file,
    retrieve_patient_entry_index,
    pascal_to_snake_case,
    retrieve_json_filenames,
    PatientNotFoundError)


@pytest.fixture
def fhir_entries() -> list:
    fhir_test_file = "tests/data/fhir_01.json"
    with open(fhir_test_file, 'r') as f:
        fhir_data = json.load(f)
        return fhir_data['entry']


class TestParseJsonFile:
    single_dict_file_path = "tests/data/single_dict.json"
    nested_dict_file_path = "tests/data/nested_data.json"
    non_existent_file_path = "tests/data/does_not_exist.json"
    invalid_json_file_path = "tests/data/invalid_file.json"

    def test_parse_json_file_returns_type_dict(self):
        data = parse_json_file(self.single_dict_file_path)
        assert isinstance(data, dict)

    def test_parse_json_file_returns_correct_key_value(self):
        expected_return_value = {"test_key": "test_value"}
        data = parse_json_file(self.single_dict_file_path)
        assert data == expected_return_value

    def test_parse_json_file_returns_correct_values_for_nested_data(self):
        # FHIR files contain nested data so we need to make sure it is
        # correctly parsed.
        expected_return_value = {
            "level_1_dict_key": {
                "level_2_dict_key": "level_2_dict_value"
            },
            "level_1_list_key": ["level_1_list_value_1",
                                 "level_1_list_value_2"],
            "level_1_list_of_dicts_key": [{"level_1_": "list_value1"},
                                          {"list_key": "list_value2"}]
        }
        data = parse_json_file(self.nested_dict_file_path)
        assert data == expected_return_value

    def test_parse_json_file_raises_exception_if_file_not_found(self):
        with pytest.raises(FileNotFoundError) as e:
            parse_json_file(self.non_existent_file_path)
        # checking the default error message makes sense, so we do not need to
        # change it.
        assert f"No such file or directory: '{self.non_existent_file_path}'" in str(
            e.value)

    def test_parse_json_file_raises_exception_if_file_not_valid_json(self, caplog):
        with pytest.raises(json.decoder.JSONDecodeError):
            parse_json_file(self.invalid_json_file_path)
        assert f"Invalid JSON file: {self.invalid_json_file_path}" in caplog.text


class TestRetreivePatientEntryIndex:
    no_patient_entry = [{"resource": {"resourceType": "Observation"}}]

    def test_retrieve_patient_entry_index_returns_type_int(self,
                                                           fhir_entries):
        index = retrieve_patient_entry_index(fhir_entries)
        assert isinstance(index, int)

    def test_retrieve_patient_entry_index_returns_correct_index(self,
                                                                fhir_entries):
        index = retrieve_patient_entry_index(fhir_entries)
        assert index == 0

    def test_retrieve_patient_entry_raises_exception_if_no_patient_entry(self):
        with pytest.raises(PatientNotFoundError) as e:
            retrieve_patient_entry_index(self.no_patient_entry)
        assert "Unable to locate Patient entry in input entry list." in str(
            e.value)


class TestPascalToSnakeCase:
    pascal = "Pascal"
    snake = "pascal"
    pascal_1 = "PascalCase"
    snake_1 = "pascal_case"
    pascal_2 = "PascalCaseTwo"
    snake_2 = "pascal_case_two"

    def test_pascal_to_snake_case_returns_type_string(self):
        snake = pascal_to_snake_case(self.pascal)
        assert isinstance(snake, str)

    def test_pascal_to_snake_case_returns_correct_value_if_uppercase(self):
        snake = pascal_to_snake_case(self.pascal)
        assert snake == "pascal"

    def test_pascal_to_snake_case_returns_correct_value_for_two_uppercases(self):
        snake = pascal_to_snake_case(self.pascal_1)
        assert snake == self.snake_1

    def test_pascal_to_snake_case_returns_correct_value_for_three_uppercases(self):
        snake = pascal_to_snake_case(self.pascal_2)
        assert snake == self.snake_2


class TestRetrieveJsonFilenames:
    directory = "tests/data/fhir_bundles"
    filenames = ["fhir_01.json", "fhir_02.json", "fhir_03.json",
                 "fhir_04.json", "fhir_05.json"]

    def test_retrieve_json_filenames_returns_type_list(self):
        files = retrieve_json_filenames(self.directory)
        assert isinstance(files, list)

    def test_retrieve_json_filenames_returns_list_of_correct_length(self):
        files = retrieve_json_filenames(self.directory)
        assert len(files) == 5

    def test_retrieve_json_filenames_returns_correct_files(self):
        retrieved_files = retrieve_json_filenames(self.directory)
        for filename in self.filenames:
            assert filename in retrieved_files

    def test_retrieve_json_filenames_ignores_non_json_files(self):
        files = retrieve_json_filenames(self.directory)
        assert len(files) == 5
        # add non-json file to directory
        with open(os.path.join(self.directory, "non_json_file.txt"), 'w') as f:
            f.write("test")
        files = retrieve_json_filenames(self.directory)
        assert len(files) == 5
        # remove non-json file from directory
        os.remove(os.path.join(self.directory, "non_json_file.txt"))
