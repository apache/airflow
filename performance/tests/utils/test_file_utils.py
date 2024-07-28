import os
from copy import deepcopy
from json.decoder import JSONDecodeError
from unittest import TestCase, mock

from utils.file_utils import (
    check_output_path,
    read_json_file,
    read_templated_json_file,
    save_output_file,
)

ENVIRONMENT_SPECIFICATIONS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "environments", "environment_specifications"
)

EXPECTED_FILE_CONTENTS = {"environment_type": "SOME_TYPE", "config_body": {}}
DIR_PATH = "path/to/output"
OUTPUT_PATH = f"{DIR_PATH}/file"
DEFAULT_FILE_NAME_NO_EXTENSION = "test_default_file"
EXTENSION = ".csv"
DEFAULT_FILE_NAME = f"{DEFAULT_FILE_NAME_NO_EXTENSION}{EXTENSION}"
JINJA_VARIABLES_DICT = {
    "jinja_variable_1": "value_1",
    "jinja_variable_2": "value_2",
    "jinja_variable_bool": "true",
}


# pylint: disable=no-self-use
class TestFileUtils(TestCase):
    def test_read_json_file(self):
        file_contents = read_json_file(
            os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "get_environment_type.json")
        )

        self.assertEqual(EXPECTED_FILE_CONTENTS, file_contents)

    def test_read_json_file_wrong_file(self):

        with self.assertRaises(JSONDecodeError):
            read_json_file(os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "not_a_valid_json.json"))

    def test_read_templated_json_file(self):

        expected_rendered_file_contents = {
            "environment_type": "SOME_TYPE",
            "config_body": {},
            "jinja_variable_1": "value_1",
            "jinja_variable_2": "value_2",
        }

        rendered_file_contents = read_templated_json_file(
            os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "templated_specification.json"),
            jinja_variables_dict=deepcopy(JINJA_VARIABLES_DICT),
        )

        self.assertEqual(expected_rendered_file_contents, rendered_file_contents)

    def test_read_templated_json_file__non_templated_file(self):

        rendered_file_contents = read_templated_json_file(
            os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "get_environment_type.json"),
            jinja_variables_dict=deepcopy(JINJA_VARIABLES_DICT),
        )

        self.assertEqual(EXPECTED_FILE_CONTENTS, rendered_file_contents)

    def test_read_templated_json_file__not_a_valid_json_file(self):

        expected_rendered_file_contents = {
            "environment_type": "SOME_TYPE",
            "config_body": {},
            "jinja_variable_1": "value_1",
            "jinja_variable_2": "value_2",
            "jinja_variable_bool": True,
        }

        rendered_file_contents = read_templated_json_file(
            os.path.join(
                ENVIRONMENT_SPECIFICATIONS_DIR,
                "templated_specification_not_a_valid_json.json",
            ),
            jinja_variables_dict=deepcopy(JINJA_VARIABLES_DICT),
        )

        self.assertEqual(expected_rendered_file_contents, rendered_file_contents)

    def test_read_templated_json_file__fill_with_nulls(self):

        expected_rendered_file_contents = {
            "environment_type": "SOME_TYPE",
            "config_body": {},
            "jinja_variable_1": "null",
            "jinja_variable_2": "null",
            "jinja_variable_bool": None,
        }

        rendered_file_contents = read_templated_json_file(
            os.path.join(
                ENVIRONMENT_SPECIFICATIONS_DIR,
                "templated_specification_not_a_valid_json.json",
            ),
            jinja_variables_dict=deepcopy(JINJA_VARIABLES_DICT),
            fill_with_nulls=True,
        )

        self.assertEqual(expected_rendered_file_contents, rendered_file_contents)

    def test_read_templated_json_file__missing_variables(self):

        non_complete_dict = deepcopy(JINJA_VARIABLES_DICT)

        non_complete_dict.pop("jinja_variable_bool")

        with self.assertRaises(ValueError):
            read_templated_json_file(
                os.path.join(
                    ENVIRONMENT_SPECIFICATIONS_DIR,
                    "templated_specification_not_a_valid_json.json",
                ),
                jinja_variables_dict=non_complete_dict,
            )

    def test_read_templated_json_file__missing_variables__fill_with_nulls(self):

        expected_rendered_file_contents = {
            "environment_type": "SOME_TYPE",
            "config_body": {},
            "jinja_variable_1": "null",
            "jinja_variable_2": "null",
            "jinja_variable_bool": None,
        }

        rendered_file_contents = read_templated_json_file(
            os.path.join(
                ENVIRONMENT_SPECIFICATIONS_DIR,
                "templated_specification_not_a_valid_json.json",
            ),
            jinja_variables_dict=None,
            fill_with_nulls=True,
        )

        self.assertEqual(expected_rendered_file_contents, rendered_file_contents)

    @mock.patch("utils.file_utils.check_output_path")
    @mock.patch("os.path.isdir", return_value=False)
    @mock.patch("os.path.join")
    def test_save_output_file(self, mock_join, mock_is_dir, mock_check_output_path):

        results_df = mock.Mock()

        save_output_file(results_df=results_df, output_path=OUTPUT_PATH, default_file_name=None)

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(OUTPUT_PATH)
        mock_join.assert_not_called()
        results_df.to_csv.assert_called_once_with(OUTPUT_PATH, index=False)

    @mock.patch("utils.file_utils.check_output_path")
    @mock.patch("os.path.isdir", return_value=True)
    @mock.patch("os.path.join")
    def test_save_output_file_dir_path(self, mock_join, mock_is_dir, mock_check_output_path):

        results_df = mock.Mock()

        save_output_file(
            results_df=results_df,
            output_path=OUTPUT_PATH,
            default_file_name=DEFAULT_FILE_NAME,
        )

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(OUTPUT_PATH)
        mock_join.assert_called_once_with(OUTPUT_PATH, DEFAULT_FILE_NAME)
        results_df.to_csv.assert_called_once_with(mock_join.return_value, index=False)

    @mock.patch("utils.file_utils.check_output_path")
    @mock.patch("os.path.isdir", return_value=True)
    @mock.patch("os.path.join")
    def test_save_output_file_dir_path_no_extension(self, mock_join, mock_is_dir, mock_check_output_path):

        results_df = mock.Mock()

        save_output_file(
            results_df=results_df,
            output_path=OUTPUT_PATH,
            default_file_name=DEFAULT_FILE_NAME_NO_EXTENSION,
        )

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(OUTPUT_PATH)
        mock_join.assert_called_once_with(OUTPUT_PATH, DEFAULT_FILE_NAME)
        results_df.to_csv.assert_called_once_with(mock_join.return_value, index=False)

    @mock.patch("utils.file_utils.check_output_path")
    @mock.patch("os.path.isdir", return_value=True)
    @mock.patch("os.path.join")
    def test_save_output_file_dir_path_no_default_file_name(
        self, mock_join, mock_is_dir, mock_check_output_path
    ):

        results_df = mock.Mock()

        with self.assertRaises(ValueError):
            save_output_file(results_df=results_df, output_path=OUTPUT_PATH, default_file_name=None)

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(OUTPUT_PATH)
        mock_join.assert_not_called()

    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("os.path.isdir")
    def test_check_output_path__existing_path(self, mock_is_dir, mock_exists):
        check_output_path(OUTPUT_PATH)
        mock_exists.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_not_called()

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.path.isdir", return_value=True)
    def test_check_output_path__non_existing_file(self, mock_is_dir, mock_exists):
        check_output_path(OUTPUT_PATH)
        mock_exists.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(DIR_PATH)

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.path.isdir", return_value=False)
    def test_check_output_path__path_does_not_exist(self, mock_is_dir, mock_exists):

        with self.assertRaises(ValueError):
            check_output_path(OUTPUT_PATH)
        mock_exists.assert_called_once_with(OUTPUT_PATH)
        mock_is_dir.assert_called_once_with(DIR_PATH)
