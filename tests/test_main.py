import pytest
import json

from main import parse_json_file


class TestParseJsonFile():
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
