import os
from copy import deepcopy
from unittest import TestCase, mock

from performance_test import PerformanceTest
from environments.base_environment import (
    State,
    DEFAULT_SLEEP_BETWEEN_CHECKS,
)

MODULE_NAME = "performance_test"
ENVIRONMENT_SPECIFICATIONS_DIR = os.path.join(
    os.path.dirname(__file__), "instances", "environment_specifications"
)

COMPOSER_ENVIRONMENT_TYPE = "COMPOSER"
INSTANCE_SPECIFICATION_FILE_PATH = "environment.json"
ELASTIC_DAG_FILE_PATH = "dag_file.py"
ELASTIC_DAG_CONFIG_FILE_PATH = "elastic_dag_conf.json"
JINJA_VARIABLES_DICT = {"jinja_variable": "variable_value"}
OUTPUT_PATH = "output_file.csv"


class TestPerformanceTest(TestCase):
    @mock.patch(
        MODULE_NAME + ".PerformanceTest.get_instance_type",
        return_value=COMPOSER_ENVIRONMENT_TYPE,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment")
    def test_init_composer_environment(
        self,
        mock_composer_environment,
        mock_get_instance_type,
    ):

        mock_composer_environment.environment_type = COMPOSER_ENVIRONMENT_TYPE

        performance_test = PerformanceTest(
            instance_specification_file_path=INSTANCE_SPECIFICATION_FILE_PATH,
            elastic_dag_path=ELASTIC_DAG_FILE_PATH,
            elastic_dag_config_file_path=ELASTIC_DAG_CONFIG_FILE_PATH,
            jinja_variables_dict=deepcopy(JINJA_VARIABLES_DICT),
        )

        self.assertEqual(performance_test.environment, mock_composer_environment.return_value)

        mock_get_instance_type.assert_called_once_with(INSTANCE_SPECIFICATION_FILE_PATH)
        mock_composer_environment.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_FILE_PATH,
            ELASTIC_DAG_CONFIG_FILE_PATH,
            JINJA_VARIABLES_DICT,
            False,
            False,
            False,
            False,
        )

    @mock.patch(
        MODULE_NAME + ".PerformanceTest.get_instance_type",
        return_value=COMPOSER_ENVIRONMENT_TYPE,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment")
    def test_init_composer_environment_no_results(
        self,
        mock_composer_environment,
        mock_get_instance_type,
    ):

        mock_composer_environment.environment_type = COMPOSER_ENVIRONMENT_TYPE

        with self.assertRaises(ValueError):
            _ = PerformanceTest(
                instance_specification_file_path=INSTANCE_SPECIFICATION_FILE_PATH,
                elastic_dag_path=ELASTIC_DAG_FILE_PATH,
                elastic_dag_config_file_path=ELASTIC_DAG_CONFIG_FILE_PATH,
            )

        mock_get_instance_type.assert_not_called()
        mock_composer_environment.assert_not_called()

    @mock.patch(
        MODULE_NAME + ".PerformanceTest.get_instance_type",
        return_value="NON_EXISTENT_TYPE",
    )
    def test_init_wrong_environment_type(self, mock_get_instance_type):
        with self.assertRaises(ValueError):
            _ = PerformanceTest(
                instance_specification_file_path=INSTANCE_SPECIFICATION_FILE_PATH,
                elastic_dag_path=ELASTIC_DAG_FILE_PATH,
                elastic_dag_config_file_path=ELASTIC_DAG_CONFIG_FILE_PATH,
                output_path=OUTPUT_PATH,
            )
        mock_get_instance_type.assert_called_once_with(INSTANCE_SPECIFICATION_FILE_PATH)

    def test_get_instance_type(self):
        file_path = os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "get_instance_type.json")

        self.assertEqual(PerformanceTest.get_instance_type(file_path), "SOME_TYPE")

    def test_get_instance_type_not_a_dict(self):
        file_path = os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "not_a_dict.json")

        with self.assertRaises(TypeError):
            PerformanceTest.get_instance_type(file_path)

    def test_get_instance_type_no_key(self):
        file_path = os.path.join(ENVIRONMENT_SPECIFICATIONS_DIR, "no_instance_type_key.json")

        with self.assertRaises(KeyError):
            PerformanceTest.get_instance_type(file_path)

    def test_get_results_object_name(self):
        result = PerformanceTest.get_results_object_name(["environment_user", "x.x.x", "aaa-bbb-ccc"])

        expected_result = "environment_user__x_x_x__aaa_bbb_ccc"

        self.assertEqual(result, expected_result)


# pylint: disable=no-member
class TestPerformanceTestWithComposerEnvironment(TestCase):
    def setUp(self):

        with mock.patch(
            MODULE_NAME + ".PerformanceTest.get_instance_type",
            return_value=COMPOSER_ENVIRONMENT_TYPE,
        ), mock.patch(MODULE_NAME + ".ComposerEnvironment") as mock_composer_environment:

            mock_composer_environment.environment_type = COMPOSER_ENVIRONMENT_TYPE

            self.performance_test = PerformanceTest(
                instance_specification_file_path=INSTANCE_SPECIFICATION_FILE_PATH,
                elastic_dag_path=ELASTIC_DAG_FILE_PATH,
                elastic_dag_config_file_path=ELASTIC_DAG_CONFIG_FILE_PATH,
                output_path=OUTPUT_PATH,
            )

            self.performance_test.environment.state = State.NONE
            self.performance_test.environment.consecutive_errors = 0

    def test_check_state(self):
        is_terminal_state_mock = mock.PropertyMock(return_value=False)
        type(self.performance_test.environment).is_terminal_state = is_terminal_state_mock

        is_retryable_mock = mock.PropertyMock(return_value=True)
        type(self.performance_test.environment).is_retryable = is_retryable_mock

        state_method_mock = mock.Mock(return_value=State.WAIT_UNTIL_READY)
        self.performance_test.environment.get_state_method = mock.Mock(return_value=state_method_mock)

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        wait_time = self.performance_test.check_state()

        name_mock.assert_called_once_with()
        is_terminal_state_mock.assert_called_once_with()
        self.performance_test.environment.get_state_method.assert_called_once_with()
        state_method_mock.assert_called_once_with()
        is_retryable_mock.assert_not_called()
        self.performance_test.environment.get_state_wait_time.assert_called_once()

        self.assertEqual(self.performance_test.environment.consecutive_errors, 0)
        self.assertEqual(self.performance_test.environment.state, State.WAIT_UNTIL_READY)
        self.assertEqual(
            wait_time,
            self.performance_test.environment.get_state_wait_time.return_value,
        )

    def test_check_state_terminal(self):
        is_terminal_state_mock = mock.PropertyMock(return_value=True)
        type(self.performance_test.environment).is_terminal_state = is_terminal_state_mock

        is_retryable_mock = mock.PropertyMock(return_value=True)
        type(self.performance_test.environment).is_retryable = is_retryable_mock

        state_method_mock = mock.Mock(return_value=State.WAIT_UNTIL_READY)
        self.performance_test.environment.get_state_method = mock.Mock(return_value=state_method_mock)

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        wait_time = self.performance_test.check_state()

        name_mock.assert_called_once_with()
        is_terminal_state_mock.assert_called_once_with()
        self.performance_test.environment.get_state_method.assert_not_called()
        state_method_mock.assert_not_called()
        is_retryable_mock.assert_not_called()
        self.performance_test.environment.get_state_wait_time.assert_not_called()

        self.assertEqual(self.performance_test.environment.consecutive_errors, 0)
        self.assertEqual(self.performance_test.environment.state, State.NONE)
        self.assertEqual(wait_time, DEFAULT_SLEEP_BETWEEN_CHECKS)

    def test_check_state_no_method(self):

        is_terminal_state_mock = mock.PropertyMock(return_value=False)
        type(self.performance_test.environment).is_terminal_state = is_terminal_state_mock

        is_retryable_mock = mock.PropertyMock(return_value=True)
        type(self.performance_test.environment).is_retryable = is_retryable_mock

        self.performance_test.environment.get_state_method = mock.Mock(return_value=None)

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        wait_time = self.performance_test.check_state()

        name_mock.assert_called_once_with()
        self.performance_test.environment.get_state_method.assert_called_once_with()
        is_terminal_state_mock.assert_called_once_with()
        is_retryable_mock.assert_not_called()
        self.performance_test.environment.get_state_wait_time.assert_not_called()

        self.assertEqual(self.performance_test.environment.consecutive_errors, 0)
        self.assertEqual(self.performance_test.environment.state, State.FAILED)
        self.assertEqual(wait_time, DEFAULT_SLEEP_BETWEEN_CHECKS)

    def test_check_state_failure_retryable(self):

        is_terminal_state_mock = mock.PropertyMock(return_value=False)
        type(self.performance_test.environment).is_terminal_state = is_terminal_state_mock

        is_retryable_mock = mock.PropertyMock(return_value=True)
        type(self.performance_test.environment).is_retryable = is_retryable_mock

        state_method_mock = mock.Mock(side_effect=ValueError("test-msg"))
        self.performance_test.environment.get_state_method = mock.Mock(return_value=state_method_mock)

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        wait_time = self.performance_test.check_state()

        name_mock.assert_called_once_with()
        is_terminal_state_mock.assert_called_once_with()
        self.performance_test.environment.get_state_method.assert_called_once_with()
        state_method_mock.assert_called_once_with()
        is_retryable_mock.assert_called_once_with()
        self.performance_test.environment.get_state_wait_time.assert_called_once_with()

        self.assertEqual(self.performance_test.environment.consecutive_errors, 1)
        self.assertEqual(self.performance_test.environment.state, State.NONE)
        self.assertEqual(
            wait_time,
            self.performance_test.environment.get_state_wait_time.return_value,
        )

    def test_check_state_failure_not_retryable(self):

        is_terminal_state_mock = mock.PropertyMock(return_value=False)
        type(self.performance_test.environment).is_terminal_state = is_terminal_state_mock

        is_retryable_mock = mock.PropertyMock(return_value=False)
        type(self.performance_test.environment).is_retryable = is_retryable_mock

        state_method_mock = mock.Mock(side_effect=ValueError("test-msg"))
        self.performance_test.environment.get_state_method = mock.Mock(return_value=state_method_mock)

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        with self.assertRaises(ValueError):
            self.performance_test.check_state()

        name_mock.assert_called_once_with()
        is_terminal_state_mock.assert_called_once_with()
        self.performance_test.environment.get_state_method.assert_called_once_with()
        state_method_mock.assert_called_once_with()
        is_retryable_mock.assert_called_once_with()
        self.performance_test.environment.get_state_wait_time.assert_not_called()

        self.assertEqual(self.performance_test.environment.consecutive_errors, 1)
        self.assertEqual(self.performance_test.environment.state, State.FAILED)

    @mock.patch(MODULE_NAME + ".check_output_path")
    def test_check_outputs_all_outputs(self, mock_check_output_path):
        self.performance_test.check_outputs()

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)

    @mock.patch(MODULE_NAME + ".check_output_path")
    def test_check_outputs_only_output_path(self, mock_check_output_path):

        self.performance_test.check_outputs()

        mock_check_output_path.assert_called_once_with(OUTPUT_PATH)

    @mock.patch(MODULE_NAME + ".PerformanceTest.get_results_object_name")
    @mock.patch(MODULE_NAME + ".save_output_file")
    def test_save_results_only_output_file(self, mock_save_output_file, mock_get_results_object_name):

        self.performance_test.environment.results = (mock.Mock(), mock.Mock())

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        self.performance_test.save_results()

        name_mock.assert_called_once_with()
        mock_get_results_object_name.assert_called_once_with(self.performance_test.environment.results[1])
        mock_save_output_file.assert_called_once_with(
            results_df=self.performance_test.environment.results[0],
            output_path=OUTPUT_PATH,
            default_file_name=mock_get_results_object_name.return_value,
        )

    @mock.patch(MODULE_NAME + ".PerformanceTest.get_results_object_name")
    @mock.patch(MODULE_NAME + ".save_output_file")
    def test_save_results_all_outputs(self, mock_save_output_file, mock_get_results_object_name):

        self.performance_test.environment.results = (mock.Mock(), mock.Mock())

        name_mock = mock.PropertyMock()
        type(self.performance_test.environment).name = name_mock

        self.performance_test.save_results()

        name_mock.assert_called_once_with()
        mock_get_results_object_name.assert_called_once_with(self.performance_test.environment.results[1])
        mock_save_output_file.assert_called_once_with(
            results_df=self.performance_test.environment.results[0],
            output_path=OUTPUT_PATH,
            default_file_name=mock_get_results_object_name.return_value,
        )


# pylint: enable=no-member
