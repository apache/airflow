import uuid
from copy import deepcopy
from unittest import TestCase, mock

import pandas as pd
from kubernetes.client import ApiClient
from parameterized import parameterized

from environments.base_environment import State
from environments.kubernetes.gke.composer.composer_environment import (
    STATS_COLLECTOR_CONTAINER_NAME,
    STATS_COLLECTOR_POD_PREFIX,
    ComposerEnvironment,
    LISTING_ENVIRONMENTS_PAGE_SIZE,
)

MODULE_NAME = "environments.kubernetes.gke.composer.composer_environment"

API_VERSION = "v1"
API_ENDPOINT = "endpoint"
FORCE_ROUTING = True
PROJECT_ID = "test_project_id"
LOCATION_ID = "test_location_id"
ZONE = "test_zone"
ENVIRONMENT_ID = "test-environment-id"
CLUSTER_ID = "test_cluster_id"
NAME = f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/environments/{ENVIRONMENT_ID}"
PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION_ID}"
GKE_CLUSTER = f"projects/{PROJECT_ID}/zones/{ZONE}/clusters/{CLUSTER_ID}"
INSTANCE_SPECIFICATION_FILE_PATH = "environment.json"
DAG_FILE_PATH = "dag_file.py"
ELASTIC_DAG_CONF_FILE_PATH = "elastic_dag_conf.json"
DEFAULT_JINJA_VARIABLES = {"jinja_variable_1": "value_1", "jinja_variable_2": "value_2"}
JINJA_VARIABLES_DICT = {
    "jinja_variable_1": "different_value",
    "jinja_variable_3": "value_3",
    "environment_id": ENVIRONMENT_ID,
    "composer_version": "2.8.3",
    "airflow_version": "2.7.3",
    "python_version": "3",
    "node_count": "4",
}
CONSTRUCTED_ENVIRONMENT_ID = "cmp-2-8-3-af2-7-3-py3-4nodes"
BUCKET_NAME = "test_bucket"
DAGS_FOLDER = "dags"
CONFIG_BODY_ENV_VARS = {
    "TEST_ENV_VAR_1": "test_env_var_value_1",
    "TEST_ENV_VAR_2": "test_env_var_value_2_a",
}
CONFIG_BODY = {
    "config": {
        "gkeCluster": GKE_CLUSTER,
        "dagGcsPrefix": f"gs://{BUCKET_NAME}/{DAGS_FOLDER}",
        "softwareConfig": {"envVariables": CONFIG_BODY_ENV_VARS},
    },
    "name": NAME,
}
SPECIFICATION = {
    "environment_type": "COMPOSER",
    "api_version": API_VERSION,
    "api_endpoint": API_ENDPOINT,
    "force_routing": FORCE_ROUTING,
    "config_body": CONFIG_BODY,
}
ELASTIC_DAG_CONF = {
    "TEST_ENV_VAR_2": "test_env_var_value_2_b",
    "TEST_ENV_VAR_3": "test_env_var_value_3",
}
RESULTS_COLUMNS = {"column_name": "column_value"}
NUMBER_OF_LOOPS = 5
NUMBER_OF_ATTEMPTS = 3


# pylint: disable=too-many-public-methods
# pylint: disable=protected-access
# pylint: disable=no-member
# pylint: disable=too-many-lines
class TestComposerEnvironment(TestCase):
    def setUp(self):
        with mock.patch("environments.kubernetes.gke.gke_based_environment.ClusterManagerClient"), mock.patch(
            "environments.kubernetes.gke.gke_based_environment.build"
        ), mock.patch(
            MODULE_NAME + ".ComposerEnvironment.load_specification"
        ) as mock_load_specification, mock.patch(
            MODULE_NAME + ".ComposerApi"
        ), mock.patch(
            MODULE_NAME + ".StorageClient"
        ):
            mock_load_specification.return_value = (
                deepcopy(SPECIFICATION),
                PROJECT_ID,
                LOCATION_ID,
                ENVIRONMENT_ID,
            )
            self.composer = ComposerEnvironment(INSTANCE_SPECIFICATION_FILE_PATH, DAG_FILE_PATH)

    @mock.patch("environments.kubernetes.gke.gke_based_environment.ClusterManagerClient")
    @mock.patch("environments.kubernetes.gke.gke_based_environment.build")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.load_specification")
    @mock.patch(MODULE_NAME + ".ComposerApi")
    @mock.patch(MODULE_NAME + ".StorageClient")
    def test_init_elastic_dag_config_file(
        self,
        mock_storage_client,
        mock_composer_api,
        mock_load_specification,
        mock_build,
        mock_cluster_manager_client,
    ):
        mock_load_specification.return_value = (
            deepcopy(SPECIFICATION),
            PROJECT_ID,
            LOCATION_ID,
            ENVIRONMENT_ID,
        )
        composer = ComposerEnvironment(
            INSTANCE_SPECIFICATION_FILE_PATH,
            DAG_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            deepcopy(JINJA_VARIABLES_DICT),
        )

        mock_load_specification.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            JINJA_VARIABLES_DICT,
        )

        self.assertEqual(composer.config_body, CONFIG_BODY)
        self.assertEqual(composer.force_routing, FORCE_ROUTING)
        self.assertEqual(composer.project_id, PROJECT_ID)
        self.assertEqual(composer.location_id, LOCATION_ID)
        self.assertEqual(composer.environment_id, ENVIRONMENT_ID)

        mock_cluster_manager_client.assert_called_once_with()
        mock_build.assert_called_once_with("compute", "v1")
        mock_composer_api.assert_called_once_with(API_VERSION, API_ENDPOINT)
        mock_storage_client.assert_called_once_with(project_id=PROJECT_ID)

    def test_construct_environment_id(self):

        environment_id = self.composer.construct_environment_id(deepcopy(JINJA_VARIABLES_DICT))
        self.assertEqual(CONSTRUCTED_ENVIRONMENT_ID, environment_id)

    def test_construct_environment_id_too_long(self):
        jinja_variables_dict = deepcopy(JINJA_VARIABLES_DICT)

        expected_result = "cmp-2-8-3-af2-7-3-py"

        jinja_variables_dict["python_version"] = (64 - len(expected_result) - 1) * "1"

        expected_result += jinja_variables_dict["python_version"]

        environment_id = self.composer.construct_environment_id(jinja_variables_dict)
        self.assertEqual(expected_result, environment_id)

    def test_validate_environment_id(self):

        self.composer.validate_environment_id(CONSTRUCTED_ENVIRONMENT_ID)

    def test_validate_environment_id_upper_case(self):
        environment_id = CONSTRUCTED_ENVIRONMENT_ID.upper()

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    def test_validate_environment_starts_with_digit(self):
        environment_id = f"1{CONSTRUCTED_ENVIRONMENT_ID}"

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    def test_validate_environment_starts_with_dash(self):
        environment_id = f"-{CONSTRUCTED_ENVIRONMENT_ID}"

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    def test_validate_environment_ends_with_dash(self):
        environment_id = f"{CONSTRUCTED_ENVIRONMENT_ID}-"

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    @parameterized.expand([".", "_", "@", "$"])
    def test_validate_environment_wrong_characters(self, forbidden_char):
        environment_id = f"a-{forbidden_char}-{CONSTRUCTED_ENVIRONMENT_ID}"

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    def test_validate_environment_id_too_long(self):
        environment_id = CONSTRUCTED_ENVIRONMENT_ID + (65 - len(CONSTRUCTED_ENVIRONMENT_ID)) * "a"

        with self.assertRaises(ValueError):
            self.composer.validate_environment_id(environment_id)

    @mock.patch("shortuuid.ShortUUID", return_value=mock.Mock())
    def test_get_random_environment_id(self, mock_shortuuid):

        random_part_length = 10
        random_part = "a" * random_part_length

        mock_shortuuid.return_value.random.return_value = random_part

        expected_result = f"{CONSTRUCTED_ENVIRONMENT_ID}-{random_part}"

        environment_id = self.composer.get_random_environment_id(CONSTRUCTED_ENVIRONMENT_ID)

        mock_shortuuid.assert_called_once_with()
        mock_shortuuid.return_value.random.assert_called_once_with(length=random_part_length)
        self.assertEqual(expected_result, environment_id)

    @mock.patch("shortuuid.ShortUUID", return_value=mock.Mock())
    def test_get_random_environment_id_too_long(self, mock_shortuuid):

        random_part_length = 10
        random_part = "a" * random_part_length

        mock_shortuuid.return_value.random.return_value = random_part

        input_environment_id = CONSTRUCTED_ENVIRONMENT_ID + "1" * 100
        expected_result = f"{input_environment_id[:53]}-{random_part}"

        environment_id = self.composer.get_random_environment_id(input_environment_id)

        mock_shortuuid.assert_called_once_with()
        mock_shortuuid.return_value.random.assert_called_once_with(length=random_part_length)
        self.assertEqual(expected_result, environment_id)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.load_specification")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_random_environment_id")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.fill_name_form")
    def test_prepare_specifications_for_multiple_test_attempts(
        self,
        mock_fill_name_form,
        mock_get_random_environment_id,
        mock_load_specification,
    ):

        mock_load_specification.return_value = (
            deepcopy(SPECIFICATION),
            PROJECT_ID,
            LOCATION_ID,
            ENVIRONMENT_ID,
        )

        environment_ids = [f"{ENVIRONMENT_ID}-{i}" for i in range(NUMBER_OF_ATTEMPTS)]
        environment_names = [
            f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/environments/{environment_id}"
            for environment_id in environment_ids
        ]
        mock_get_random_environment_id.side_effect = environment_ids
        mock_fill_name_form.side_effect = environment_names

        parallel, sequential = ComposerEnvironment.prepare_specifications_for_multiple_test_attempts(
            NUMBER_OF_ATTEMPTS,
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            deepcopy(JINJA_VARIABLES_DICT),
            randomize_environment_name=True,
        )

        mock_load_specification.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            JINJA_VARIABLES_DICT,
        )

        mock_get_random_environment_id.has_calls([mock.call(ENVIRONMENT_ID)])
        self.assertEqual(mock_get_random_environment_id.call_count, NUMBER_OF_ATTEMPTS)

        fill_name_calls = [
            mock.call(PROJECT_ID, LOCATION_ID, environment_id) for environment_id in environment_ids
        ]
        mock_fill_name_form.has_calls(fill_name_calls)
        self.assertEqual(mock_fill_name_form.call_count, NUMBER_OF_ATTEMPTS)

        self.assertEqual(sequential, [])

        expected_environments = []
        for i, environment_name in enumerate(environment_names):
            specification = deepcopy(SPECIFICATION)
            specification["config_body"]["name"] = environment_name
            expected_environments.append((specification, environment_ids[i]))

        self.assertEqual(parallel, expected_environments)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.load_specification")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_random_environment_id")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.fill_name_form")
    def test_prepare_specifications_for_multiple_test_attempts__private_envs(
        self,
        mock_fill_name_form,
        mock_get_random_environment_id,
        mock_load_specification,
    ):

        private_specification = deepcopy(SPECIFICATION)

        private_specification["config_body"]["config"]["privateEnvironmentConfig"] = {
            "enablePrivateEnvironment": True
        }

        mock_load_specification.return_value = (
            private_specification,
            PROJECT_ID,
            LOCATION_ID,
            ENVIRONMENT_ID,
        )

        environment_ids = [f"{ENVIRONMENT_ID}-{i}" for i in range(NUMBER_OF_ATTEMPTS)]
        environment_names = [
            f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/environments/{environment_id}"
            for environment_id in environment_ids
        ]
        mock_get_random_environment_id.side_effect = environment_ids
        mock_fill_name_form.side_effect = environment_names

        parallel, sequential = ComposerEnvironment.prepare_specifications_for_multiple_test_attempts(
            NUMBER_OF_ATTEMPTS,
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            deepcopy(JINJA_VARIABLES_DICT),
        )

        mock_load_specification.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            JINJA_VARIABLES_DICT,
        )

        mock_get_random_environment_id.has_calls([mock.call(ENVIRONMENT_ID)])
        self.assertEqual(mock_get_random_environment_id.call_count, NUMBER_OF_ATTEMPTS)

        fill_name_calls = [
            mock.call(PROJECT_ID, LOCATION_ID, environment_id) for environment_id in environment_ids
        ]
        mock_fill_name_form.has_calls(fill_name_calls)
        self.assertEqual(mock_fill_name_form.call_count, NUMBER_OF_ATTEMPTS)

        self.assertEqual(parallel, [])

        expected_environments = []
        for i, environment_name in enumerate(environment_names):
            specification = deepcopy(private_specification)
            specification["config_body"]["name"] = environment_name
            expected_environments.append((specification, environment_ids[i]))

        self.assertEqual(sequential, expected_environments)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.load_specification")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_random_environment_id")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.fill_name_form")
    def test_prepare_specifications_for_multiple_test_attempts__no_random_names(
        self,
        mock_fill_name_form,
        mock_get_random_environment_id,
        mock_load_specification,
    ):

        mock_load_specification.return_value = (
            deepcopy(SPECIFICATION),
            PROJECT_ID,
            LOCATION_ID,
            ENVIRONMENT_ID,
        )

        mock_fill_name_form.side_effect = [NAME] * NUMBER_OF_ATTEMPTS

        parallel, sequential = ComposerEnvironment.prepare_specifications_for_multiple_test_attempts(
            NUMBER_OF_ATTEMPTS,
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            deepcopy(JINJA_VARIABLES_DICT),
            randomize_environment_name=False,
        )

        mock_load_specification.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            JINJA_VARIABLES_DICT,
        )

        mock_get_random_environment_id.assert_not_called()

        fill_name_calls = [mock.call(PROJECT_ID, LOCATION_ID, ENVIRONMENT_ID)] * NUMBER_OF_ATTEMPTS
        mock_fill_name_form.has_calls(fill_name_calls)
        self.assertEqual(mock_fill_name_form.call_count, NUMBER_OF_ATTEMPTS)

        self.assertEqual(sequential, [])

        expected_environments = [(SPECIFICATION, ENVIRONMENT_ID)] * NUMBER_OF_ATTEMPTS
        self.assertEqual(parallel, expected_environments)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.load_specification")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_random_environment_id")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.fill_name_form")
    def test_prepare_specifications_for_multiple_test_attempts__reraises_exception(
        self,
        mock_fill_name_form,
        mock_get_random_environment_id,
        mock_load_specification,
    ):

        mock_load_specification.side_effect = ValueError("error")

        with self.assertRaises(ValueError):
            ComposerEnvironment.prepare_specifications_for_multiple_test_attempts(
                NUMBER_OF_ATTEMPTS,
                INSTANCE_SPECIFICATION_FILE_PATH,
                ELASTIC_DAG_CONF_FILE_PATH,
                deepcopy(JINJA_VARIABLES_DICT),
                randomize_environment_name=False,
            )

        mock_load_specification.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            JINJA_VARIABLES_DICT,
        )

        mock_get_random_environment_id.assert_not_called()
        mock_fill_name_form.assert_not_called()

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_default_jinja_variables_values")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.construct_environment_id")
    @mock.patch(MODULE_NAME + ".read_templated_json_file")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_config_body_with_elastic_dag_env_variables")
    @mock.patch(MODULE_NAME + ".add_perf_start_date_env_to_conf")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.retrieve_ids_from_name",
        return_value=(PROJECT_ID, LOCATION_ID, ENVIRONMENT_ID),
    )
    def test_load_specification(
        self,
        mock_retrieve_ids_from_name,
        mock_validate_elastic_dag_conf,
        mock_add_perf_start_date_env_to_conf,
        mock_update_config_body_with_elastic_dag_env_variables,
        mock_check_and_adjust_config_body,
        mock_read_templated_json_file,
        mock_construct_environment_id,
        mock_get_default_jinja_variables_values,
    ):
        mock_get_default_jinja_variables_values.return_value = deepcopy(DEFAULT_JINJA_VARIABLES)
        mock_read_templated_json_file.return_value = deepcopy(SPECIFICATION)

        specification, project_id, location_id, environment_id = self.composer.load_specification(
            INSTANCE_SPECIFICATION_FILE_PATH,
            ELASTIC_DAG_CONF_FILE_PATH,
            deepcopy(JINJA_VARIABLES_DICT),
        )

        jinja_variables_dict = {**DEFAULT_JINJA_VARIABLES, **JINJA_VARIABLES_DICT}

        mock_get_default_jinja_variables_values.assert_called_once_with()
        mock_construct_environment_id.assert_not_called()

        mock_read_templated_json_file.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH, jinja_variables_dict
        )
        mock_check_and_adjust_config_body.assert_called_once_with(CONFIG_BODY)
        self.assertEqual(specification, SPECIFICATION)
        self.assertEqual(project_id, PROJECT_ID)
        self.assertEqual(location_id, LOCATION_ID)
        self.assertEqual(environment_id, ENVIRONMENT_ID)

        mock_update_config_body_with_elastic_dag_env_variables.assert_called_once_with(
            CONFIG_BODY, ELASTIC_DAG_CONF_FILE_PATH
        )
        mock_add_perf_start_date_env_to_conf.assert_called_once_with(CONFIG_BODY_ENV_VARS)
        mock_validate_elastic_dag_conf.assert_called_once_with(CONFIG_BODY_ENV_VARS)

        mock_retrieve_ids_from_name.assert_called_once_with(NAME)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_default_jinja_variables_values")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.construct_environment_id",
        return_value=CONSTRUCTED_ENVIRONMENT_ID,
    )
    @mock.patch(
        MODULE_NAME + ".read_templated_json_file",
        return_value=["api_version", "config_body", "api_endpoint", "force_routing"],
    )
    def test_load_specification_not_a_dict(
        self,
        mock_read_templated_json_file,
        mock_construct_environment_id,
        mock_get_default_jinja_variables_values,
    ):
        mock_get_default_jinja_variables_values.return_value = deepcopy(DEFAULT_JINJA_VARIABLES)
        with self.assertRaises(TypeError):
            self.composer.load_specification(INSTANCE_SPECIFICATION_FILE_PATH)
        mock_get_default_jinja_variables_values.assert_called_once_with()
        # TODO: cannot change the arguments of call due to jinja_variables_to_use dict changing
        #  during load_specification method call
        mock_construct_environment_id.assert_called_once()
        expected_jinja_variables = {
            **DEFAULT_JINJA_VARIABLES,
            **{"environment_id": CONSTRUCTED_ENVIRONMENT_ID},
        }
        mock_read_templated_json_file.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH, expected_jinja_variables
        )

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_default_jinja_variables_values")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.construct_environment_id",
        return_value=CONSTRUCTED_ENVIRONMENT_ID,
    )
    @mock.patch(MODULE_NAME + ".read_templated_json_file")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_config_body_with_elastic_dag_env_variables")
    @mock.patch(MODULE_NAME + ".add_perf_start_date_env_to_conf")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.retrieve_ids_from_name",
        return_value=(PROJECT_ID, LOCATION_ID, ENVIRONMENT_ID),
    )
    def test_load_specification_default_values(
        self,
        mock_retrieve_ids_from_name,
        mock_validate_elastic_dag_conf,
        mock_add_perf_start_date_env_to_conf,
        mock_update_config_body_with_elastic_dag_env_variables,
        mock_check_and_adjust_config_body,
        mock_read_templated_json_file,
        mock_construct_environment_id,
        mock_get_default_jinja_variables_values,
    ):
        mock_get_default_jinja_variables_values.return_value = deepcopy(DEFAULT_JINJA_VARIABLES)
        mock_read_templated_json_file.return_value = {"config_body": deepcopy(CONFIG_BODY)}

        specification, project_id, location_id, environment_id = self.composer.load_specification(
            INSTANCE_SPECIFICATION_FILE_PATH
        )

        expected_jinja_variables = {
            **DEFAULT_JINJA_VARIABLES,
            **{"environment_id": CONSTRUCTED_ENVIRONMENT_ID},
        }

        mock_get_default_jinja_variables_values.assert_called_once_with()
        mock_construct_environment_id.assert_called_once()
        mock_read_templated_json_file.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH, expected_jinja_variables
        )
        mock_check_and_adjust_config_body.assert_called_once_with(CONFIG_BODY)
        self.assertEqual(specification, {"config_body": CONFIG_BODY})
        self.assertEqual(project_id, PROJECT_ID)
        self.assertEqual(location_id, LOCATION_ID)
        self.assertEqual(environment_id, ENVIRONMENT_ID)

        mock_update_config_body_with_elastic_dag_env_variables.assert_not_called()
        mock_add_perf_start_date_env_to_conf.assert_called_once_with(CONFIG_BODY_ENV_VARS)
        mock_validate_elastic_dag_conf.assert_called_once_with(CONFIG_BODY_ENV_VARS)

        mock_retrieve_ids_from_name.assert_called_once_with(NAME)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_default_jinja_variables_values")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.construct_environment_id",
        return_value=CONSTRUCTED_ENVIRONMENT_ID,
    )
    @mock.patch(
        MODULE_NAME + ".read_templated_json_file",
        return_value={
            "api_version": API_VERSION,
            "api_endpoint": API_ENDPOINT,
            "force_routing": FORCE_ROUTING,
        },
    )
    def test_load_specification_no_config_body_key(
        self,
        mock_read_templated_json_file,
        mock_construct_environment_id,
        mock_get_default_jinja_variables_values,
    ):
        mock_get_default_jinja_variables_values.return_value = deepcopy(DEFAULT_JINJA_VARIABLES)

        with self.assertRaises(KeyError):
            self.composer.load_specification(INSTANCE_SPECIFICATION_FILE_PATH)
        mock_get_default_jinja_variables_values.assert_called_once_with()
        mock_construct_environment_id.assert_called_once()
        expected_jinja_variables = {
            **DEFAULT_JINJA_VARIABLES,
            **{"environment_id": CONSTRUCTED_ENVIRONMENT_ID},
        }
        mock_read_templated_json_file.assert_called_once_with(
            INSTANCE_SPECIFICATION_FILE_PATH, expected_jinja_variables
        )

    def test_check_and_adjust_config_body(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body["config"]["softwareConfig"].pop("envVariables")

        self.composer.check_and_adjust_config_body(config_body)

        self.assertEqual(config_body["config"]["softwareConfig"]["envVariables"], {})

    def test_check_and_adjust_config_body_not_a_dict(self):

        with self.assertRaises(TypeError):
            self.composer.check_and_adjust_config_body(["config", "name"])

    def test_check_and_adjust_config_body_no_name_key(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body.pop("name")

        with self.assertRaises(KeyError):
            self.composer.check_and_adjust_config_body(config_body)

    def test_check_and_adjust_config_body_no_config_key(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body.pop("config")

        with self.assertRaises(KeyError):
            self.composer.check_and_adjust_config_body(config_body)

    def test_check_and_adjust_config_body_config_not_a_dict(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body["config"] = []

        with self.assertRaises(TypeError):
            self.composer.check_and_adjust_config_body(config_body)

    def test_check_and_adjust_config_body_no_software_config_key(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body["config"].pop("softwareConfig")

        with self.assertRaises(KeyError):
            self.composer.check_and_adjust_config_body(config_body)

    def test_check_and_adjust_config_body_software_config_not_a_dict(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body["config"]["softwareConfig"] = []

        with self.assertRaises(TypeError):
            self.composer.check_and_adjust_config_body(config_body)

    def test_check_and_adjust_config_body_env_variables_not_a_dict(self):

        config_body = deepcopy(CONFIG_BODY)
        config_body["config"]["softwareConfig"]["envVariables"] = []

        with self.assertRaises(TypeError):
            self.composer.check_and_adjust_config_body(config_body)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.validate_environment_id")
    def test_retrieve_ids_from_name(self, mock_validate_environment_id):

        project_id, location_id, environment_id = self.composer.retrieve_ids_from_name(NAME)

        mock_validate_environment_id.assert_called_once_with(ENVIRONMENT_ID)

        self.assertEqual(project_id, PROJECT_ID)
        self.assertEqual(location_id, LOCATION_ID)
        self.assertEqual(environment_id, ENVIRONMENT_ID)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.validate_environment_id")
    def test_retrieve_ids_from_name_too_many_components(self, mock_validate_environment_id):

        with self.assertRaises(ValueError):
            self.composer.retrieve_ids_from_name("a/b/c/d/e/f/g")
        mock_validate_environment_id.assert_not_called()

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.validate_environment_id")
    def test_retrieve_ids_from_name_too_few_components(self, mock_validate_environment_id):

        with self.assertRaises(ValueError):
            self.composer.retrieve_ids_from_name("a/b/c/d/e")
        mock_validate_environment_id.assert_not_called()

    @mock.patch(MODULE_NAME + ".read_json_file")
    def test_update_config_body_with_elastic_dag_env_variables(self, mock_read_json_file):
        mock_read_json_file.return_value = deepcopy(ELASTIC_DAG_CONF)

        expected_result = deepcopy(CONFIG_BODY_ENV_VARS)
        expected_result.update(ELASTIC_DAG_CONF)

        self.composer.update_config_body_with_elastic_dag_env_variables(
            self.composer.config_body, ELASTIC_DAG_CONF_FILE_PATH
        )

        mock_read_json_file.assert_called_once_with(ELASTIC_DAG_CONF_FILE_PATH)

        self.assertEqual(self.composer.get_env_variables(), expected_result)

    @mock.patch(MODULE_NAME + ".read_json_file", return_value=[])
    def test_update_config_body_with_elastic_dag_env_variables_not_a_dict(self, _):

        with self.assertRaises(TypeError):
            self.composer.update_config_body_with_elastic_dag_env_variables(
                self.composer.config_body, ELASTIC_DAG_CONF_FILE_PATH
            )

    @parameterized.expand(
        [
            (State.DONE, "RUNNING"),
            (State.DONE, "ERROR"),
            (State.FAILED, "RUNNING"),
            (State.FAILED, "ERROR"),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    def test_is_terminal_state_and_deletable(
        self, env_state, composer_state, mock_get_state, mock_check_if_exists
    ):

        self.composer.state = env_state
        mock_get_state.return_value = composer_state

        returned_value = self.composer.is_terminal_state_and_deletable

        mock_check_if_exists.assert_called_once_with()
        mock_get_state.assert_called_once_with()

        self.assertEqual(returned_value, True)

    @parameterized.expand(["RUNNING", "ERROR"])
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    def test_is_terminal_state_and_deletable__not_terminal_state(
        self, composer_state, mock_get_state, mock_check_if_exists
    ):

        self.composer.state = State.FAILED_DO_NOT_DELETE
        mock_get_state.return_value = composer_state

        returned_value = self.composer.is_terminal_state_and_deletable

        mock_check_if_exists.assert_called_once_with()
        mock_get_state.assert_called_once_with()

        self.assertEqual(returned_value, False)

    @parameterized.expand(["DELETING", "CREATING", "UPDATING", "STATE_UNSPECIFIED"])
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    def test_is_terminal_state_and_deletable__wrong_composer_state(
        self, composer_state, mock_get_state, mock_check_if_exists
    ):

        self.composer.state = State.FAILED
        mock_get_state.return_value = composer_state

        returned_value = self.composer.is_terminal_state_and_deletable

        mock_check_if_exists.assert_called_once_with()
        mock_get_state.assert_called_once_with()

        self.assertEqual(returned_value, False)

    @parameterized.expand(
        [
            (State.DONE, "RUNNING"),
            (State.DONE, "ERROR"),
            (State.FAILED, "RUNNING"),
            (State.FAILED, "ERROR"),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=False)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    def test_is_terminal_state_and_deletable__does_not_exist(
        self, env_state, composer_state, mock_get_state, mock_check_if_exists
    ):

        self.composer.state = env_state
        mock_get_state.return_value = composer_state

        returned_value = self.composer.is_terminal_state_and_deletable

        mock_check_if_exists.assert_called_once_with()
        mock_get_state.assert_not_called()

        self.assertEqual(returned_value, False)

    @parameterized.expand(
        [
            ("no_flags", False, False),
            ("reuse_if_exists_flag", True, False),
            ("delete_if_exists_flag", False, True),
            ("both_flags", True, True),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=False)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    def test_prepare_environment_does_not_exist(
        self, _, reuse_if_exists, delete_if_exists, mock_get_state, mock_check_if_exists
    ):

        self.composer.reuse_if_exists = reuse_if_exists
        self.composer.delete_if_exists = delete_if_exists

        returned_state = self.composer.prepare_environment()

        self.assertEqual(returned_state, State.WAIT_UNTIL_READY)

        mock_check_if_exists.assert_called_once_with()
        self.composer.composer_api.create_environment.assert_called_once_with(PARENT, CONFIG_BODY)
        mock_get_state.assert_not_called()

    @parameterized.expand(
        [
            ("no_flags", False, False),
            ("reuse_if_exists_flag", True, False),
            ("delete_if_exists_flag", False, True),
            ("both_flags", True, True),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state", return_value="DELETING")
    def test_prepare_environment_deleting_state(
        self, _, reuse_if_exists, delete_if_exists, mock_get_state, mock_check_if_exists
    ):

        self.composer.reuse_if_exists = reuse_if_exists
        self.composer.delete_if_exists = delete_if_exists

        returned_state = self.composer.prepare_environment()

        self.assertEqual(returned_state, State.DELETING_ENV)

        mock_check_if_exists.assert_called_once_with()
        self.composer.composer_api.create_environment.assert_not_called()
        mock_get_state.assert_called_once_with()

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state", return_value="RUNNING")
    def test_prepare_environment_exists_no_flags_no_deleting_state(
        self, mock_get_state, mock_check_if_exists
    ):
        returned_state = self.composer.prepare_environment()

        self.assertEqual(returned_state, State.FAILED_DO_NOT_DELETE)

        mock_check_if_exists.assert_called_once_with()
        self.composer.composer_api.create_environment.assert_not_called()
        mock_get_state.assert_called_once_with()

    @parameterized.expand([("reuse_if_exists_flag", True, False), ("both_flags", True, True)])
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state", return_value="RUNNING")
    def test_prepare_environment_reuse_if_exists_flag_no_deleting_state(
        self, _, reuse_if_exists, delete_if_exists, mock_get_state, mock_check_if_exists
    ):

        self.composer.reuse_if_exists = reuse_if_exists
        self.composer.delete_if_exists = delete_if_exists

        returned_state = self.composer.prepare_environment()

        self.assertEqual(returned_state, State.WAIT_UNTIL_READY)

        mock_check_if_exists.assert_called_once_with()
        self.composer.composer_api.create_environment.assert_not_called()
        mock_get_state.assert_called_once_with()

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_if_exists", return_value=True)
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state", return_value="RUNNING")
    def test_prepare_environment_delete_if_exists_flag_no_deleting_state(
        self, mock_get_state, mock_check_if_exists
    ):

        self.composer.delete_if_exists = True

        returned_state = self.composer.prepare_environment()

        self.assertEqual(returned_state, State.WAIT_UNTIL_CAN_BE_DELETED)

        mock_check_if_exists.assert_called_once_with()
        self.composer.composer_api.create_environment.assert_not_called()
        mock_get_state.assert_called_once_with()

    def test_check_if_exists(self):

        listing_responses = [
            {
                "environments": [{"name": "environment_1"}, {"name": "environment_2"}],
                "nextPageToken": "token_1",
            },
            {
                "environments": [{"name": "environment_3"}, {"name": NAME}],
                "nextPageToken": "token_2",
            },
            {"environments": [{"name": "environment_4"}, {"name": "environment_5"}]},
        ]

        composer_api_mock = mock.MagicMock(**{"list_environments.side_effect": listing_responses})

        self.composer.composer_api = composer_api_mock

        result = self.composer.check_if_exists()

        listing_calls = [
            mock.call(parent=PARENT, page_size=LISTING_ENVIRONMENTS_PAGE_SIZE, page_token=None),
            mock.call(
                parent=PARENT,
                page_size=LISTING_ENVIRONMENTS_PAGE_SIZE,
                page_token="token_1",
            ),
        ]

        composer_api_mock.list_environments.assert_has_calls(listing_calls)

        self.assertEqual(composer_api_mock.list_environments.call_count, 2)
        self.assertEqual(result, True)

    def test_check_if_exists_does_not_exist(self):

        listing_responses = [
            {
                "environments": [{"name": "environment_1"}, {"name": "environment_2"}],
                "nextPageToken": "token_1",
            },
            {
                "environments": [{"name": "environment_3"}, {"name": "environment_4"}],
                "nextPageToken": "token_2",
            },
            {"environments": [{"name": "environment_5"}]},
        ]

        composer_api_mock = mock.MagicMock(**{"list_environments.side_effect": listing_responses})

        self.composer.composer_api = composer_api_mock

        result = self.composer.check_if_exists()

        listing_calls = [
            mock.call(parent=PARENT, page_size=LISTING_ENVIRONMENTS_PAGE_SIZE, page_token=None),
            mock.call(
                parent=PARENT,
                page_size=LISTING_ENVIRONMENTS_PAGE_SIZE,
                page_token="token_1",
            ),
            mock.call(
                parent=PARENT,
                page_size=LISTING_ENVIRONMENTS_PAGE_SIZE,
                page_token="token_2",
            ),
        ]

        composer_api_mock.list_environments.assert_has_calls(listing_calls)

        self.assertEqual(composer_api_mock.list_environments.call_count, 3)
        self.assertEqual(result, False)

    def test_check_if_exists_empty_response(self):

        composer_api_mock = mock.MagicMock(**{"list_environments.return_value": {}})

        self.composer.composer_api = composer_api_mock

        result = self.composer.check_if_exists()

        composer_api_mock.list_environments.assert_called_once_with(
            parent=PARENT, page_size=LISTING_ENVIRONMENTS_PAGE_SIZE, page_token=None
        )

        self.assertEqual(result, False)

    def test_get_state(self):
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": {"state": "RUNNING"}})

        self.composer.composer_api = composer_api_mock
        expected_state = self.composer.get_state()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        self.assertEqual(expected_state, "RUNNING")

    @parameterized.expand(
        [
            ("creating_env_wait_until_ready", "CREATING", State.WAIT_UNTIL_READY),
            (
                "creating_env_wait_until_can_be_deleted",
                "CREATING",
                State.WAIT_UNTIL_CAN_BE_DELETED,
            ),
            ("updating_env_wait_until_ready", "UPDATING", State.WAIT_UNTIL_READY),
            (
                "updating_env_wait_until_can_be_deleted",
                "UPDATING",
                State.WAIT_UNTIL_CAN_BE_DELETED,
            ),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.delete_environment")
    def test_is_composer_instance_ready__not_ready(
        self, _, composer_state, env_state, mock_delete_environment, mock_get_state
    ):
        mock_get_state.return_value = composer_state
        self.composer.state = env_state

        returned_state = self.composer.is_composer_instance_ready()

        self.assertEqual(returned_state, env_state)

        mock_get_state.assert_called_once_with()
        mock_delete_environment.assert_not_called()

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state", return_value="RUNNING")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.delete_environment")
    def test_is_composer_instance_ready__running__wait_until_ready(
        self, mock_delete_environment, mock_get_state
    ):

        self.composer.state = State.WAIT_UNTIL_READY

        returned_state = self.composer.is_composer_instance_ready()

        self.assertEqual(returned_state, State.UPDATE_ENV_INFO)

        mock_get_state.assert_called_once_with()
        mock_delete_environment.assert_not_called()

    @parameterized.expand(
        [
            ("deleting", "DELETING"),
            ("error", "ERROR"),
            ("unspecified", "STATE_UNSPECIFIED"),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.delete_environment")
    def test_is_composer_instance_ready__not_running__wait_until_ready(
        self, _, composer_state, mock_delete_environment, mock_get_state
    ):

        mock_get_state.return_value = composer_state
        self.composer.state = State.WAIT_UNTIL_READY

        returned_state = self.composer.is_composer_instance_ready()

        self.assertEqual(returned_state, State.FAILED)

        mock_get_state.assert_called_once_with()
        mock_delete_environment.assert_not_called()

    @parameterized.expand([("deleting", "RUNNING"), ("error", "ERROR")])
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.delete_environment")
    def test_is_composer_instance_ready__deletable__wait_until_can_be_deleted(
        self, _, composer_state, mock_delete_environment, mock_get_state
    ):

        mock_get_state.return_value = composer_state
        self.composer.state = State.WAIT_UNTIL_CAN_BE_DELETED

        returned_state = self.composer.is_composer_instance_ready()

        self.assertEqual(returned_state, State.DELETING_ENV)

        mock_get_state.assert_called_once_with()
        mock_delete_environment.assert_called_once_with()

    @parameterized.expand([("deleting", "DELETING"), ("unspecified", "STATE_UNSPECIFIED")])
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.get_state")
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.delete_environment")
    def test_is_composer_instance_ready__not_deletable__wait_until_can_be_deleted(
        self, _, composer_state, mock_delete_environment, mock_get_state
    ):

        mock_get_state.return_value = composer_state
        self.composer.state = State.WAIT_UNTIL_CAN_BE_DELETED

        returned_state = self.composer.is_composer_instance_ready()

        self.assertEqual(returned_state, State.FAILED)

        mock_get_state.assert_called_once_with()
        mock_delete_environment.assert_not_called()

    def test_delete_environment(self):

        self.composer.delete_environment()

        self.composer.composer_api.delete_environment.assert_called_once_with(NAME)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=False,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=True,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_force_routing_public_environment(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.composer_api = composer_api_mock

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_not_called()
        mock_validate_elastic_dag_conf.assert_not_called()
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_called_once_with()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=False
        )

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=True,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=True,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_force_routing_private_environment_public_endpoint(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.composer_api = composer_api_mock

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_not_called()
        mock_validate_elastic_dag_conf.assert_not_called()
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_not_called()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=True
        )

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=True,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=True,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_no_force_routing_private_environment_public_endpoint(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.force_routing = False
        self.composer.composer_api = composer_api_mock

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_not_called()
        mock_validate_elastic_dag_conf.assert_not_called()
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_called_once_with()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=False
        )

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=True,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=False,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_no_force_routing_private_environment_no_public_endpoint(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.force_routing = False
        self.composer.composer_api = composer_api_mock

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_not_called()
        mock_validate_elastic_dag_conf.assert_not_called()
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_called_once_with()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=True
        )

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=False,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=True,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_reuse_if_exists(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.composer_api = composer_api_mock

        self.composer.reuse_if_exists = True

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_called_once_with(new_config)
        mock_validate_elastic_dag_conf.assert_called_once_with(new_env_variables)
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_called_once_with()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=False
        )

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)

    @mock.patch(MODULE_NAME + ".ComposerEnvironment.check_and_adjust_config_body")
    @mock.patch(MODULE_NAME + ".validate_elastic_dag_conf")
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_private_ip_enabled",
        return_value=False,
    )
    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.check_if_public_endpoint_enabled",
        return_value=True,
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_reuse_if_exists_and_reset_environment(
        self,
        mock_update_remote_runner_provider,
        mock_check_if_public_endpoint_enabled,
        mock_check_if_private_ip_enabled,
        mock_validate_elastic_dag_conf,
        mock_check_and_adjust_config_body,
    ):

        new_env_variables = {"new_var": "var_value"}
        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["envVariables"] = new_env_variables
        new_config["config"]["softwareConfig"]["imageVersion"] = "composer-2.4.0-airflow-2.5.3"
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.composer_api = composer_api_mock

        self.composer.reuse_if_exists = True
        self.composer.reset_environment = True

        self.composer.state = State.UPDATE_ENV_INFO

        state = self.composer._update_environment_info()

        composer_api_mock.get_environment.assert_called_once_with(NAME)
        mock_check_and_adjust_config_body.assert_called_once_with(new_config)
        mock_validate_elastic_dag_conf.assert_called_once_with(new_env_variables)
        self.assertEqual(self.composer.config_body, new_config)

        mock_check_if_public_endpoint_enabled.assert_called_once_with()
        mock_check_if_private_ip_enabled.assert_called_once_with()

        mock_update_remote_runner_provider.assert_called_once_with(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=False
        )

        self.assertEqual(state, State.RESET_ENV)

    @parameterized.expand(
        [
            ("composer-2.4.0-airflow-2.5.3", "composer"),
            ("composer-3.0.0-preview.3-airflow-2.5.3", "airflow"),
        ]
    )
    @mock.patch(MODULE_NAME + ".ComposerEnvironment.update_remote_runner_provider")
    def test_update_environment_info_namespace_prefix(
        self, image_version, expected_namespace_prefix, mock_update_remote_runner_provider
    ):

        new_config = deepcopy(CONFIG_BODY)
        new_config["config"]["softwareConfig"]["imageVersion"] = image_version
        new_config["config"]["privateEnvironmentConfig"] = {}
        composer_api_mock = mock.MagicMock(**{"get_environment.return_value": new_config})
        self.composer.composer_api = composer_api_mock
        self.composer.reuse_if_exists = False

        state = self.composer._update_environment_info()

        self.assertEqual(state, State.DEPLOY_STATS_COLLECTOR)
        self.assertEqual(self.composer.namespace_prefix, expected_namespace_prefix)

    @mock.patch(MODULE_NAME + ".CustomObjectsApi", autospec=True)
    def test_deploy_stats_collector_composer2(
        self,
        mock_custom_objects_api,
    ):
        self.composer.config_body = {
            "config": {
                "softwareConfig": {
                    "imageVersion": "composer-2.4.3-airflow-2.6.3",
                },
            },
        }
        mock_core_api = mock.MagicMock(api_client=ApiClient())
        self.composer.remote_runner_provider = mock.MagicMock(
            **{
                "get_kubernetes_apis_in_isolated_context.return_value.__enter__.return_value": (
                    mock_core_api,
                    mock.MagicMock(),
                    mock.MagicMock(),
                ),
                "find_namespace_with_a_prefix.return_value": "composer-2-4-3-airflow-2-6-3-aaabbb",
            }
        )

        def mock_get_namespaced_custom_object(group, version, plural, name, namespace):
            self.assertEqual(group, "composer.cloud.google.com")
            self.assertEqual(version, "v1beta1")
            self.assertEqual(plural, "airflowworkersets")
            self.assertEqual(name, "airflow-worker")
            self.assertEqual(namespace, "composer-2-4-3-airflow-2-6-3-aaabbb")
            return {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "airflow-worker",
                                    "image": "image-worker-scheduler",
                                    "env": [
                                        {
                                            "name": "ENV1",
                                            "value": "VALUE1",
                                        }
                                    ],
                                }
                            ],
                        },
                    },
                },
            }

        mock_custom_objects_api.return_value.get_namespaced_custom_object.side_effect = (
            mock_get_namespaced_custom_object
        )

        state = self.composer._deploy_stats_collector()

        mock_core_api.create_namespaced_pod.assert_called_with(
            body={
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "stats-collector",
                    "namespace": "composer-2-4-3-airflow-2-6-3-aaabbb",
                },
                "spec": {
                    "containers": [
                        {
                            "name": "stats-collector",
                            "image": "image-worker-scheduler",
                            "args": ["stats-collector"],
                            "env": [
                                {
                                    "name": "ENV1",
                                    "value": "VALUE1",
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": "airflow-config",
                                    "mountPath": "/etc/airflow/airflow_cfg",
                                }
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "airflow-config",
                            "configMap": {
                                "name": "airflow-configmap",
                            },
                        }
                    ],
                },
            },
            namespace="composer-2-4-3-airflow-2-6-3-aaabbb",
        )
        self.assertEqual(state, State.UPLOAD_DAG)

    @mock.patch(MODULE_NAME + ".CustomObjectsApi", autospec=True)
    def test_deploy_stats_collector_composer3(
        self,
        mock_custom_objects_api,
    ):
        self.composer.config_body = {
            "config": {
                "softwareConfig": {
                    "imageVersion": "composer-3.0.0-preview.3-airflow-2.6.3",
                },
            },
        }
        mock_core_api = mock.MagicMock(api_client=ApiClient())
        self.composer.remote_runner_provider = mock.MagicMock(
            **{
                "get_kubernetes_apis_in_isolated_context.return_value.__enter__.return_value": (
                    mock_core_api,
                    mock.MagicMock(),
                    mock.MagicMock(),
                ),
                "find_namespace_with_a_prefix.return_value": "composer-3-0-0-airflow-2-6-3-aaabbb",
            }
        )

        def mock_get_namespaced_custom_object(group, version, plural, name, namespace):
            self.assertEqual(group, "composer.cloud.google.com")
            self.assertEqual(version, "v1beta1")
            self.assertEqual(plural, "airflowworkersets")
            self.assertEqual(name, "airflow-worker")
            self.assertEqual(namespace, "composer-3-0-0-airflow-2-6-3-aaabbb")
            return {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "airflow-worker",
                                    "image": "image-worker-scheduler",
                                    "env": [
                                        {
                                            "name": "ENV1",
                                            "value": "VALUE1",
                                        },
                                        {
                                            "name": "SQL_HOST",
                                            "value": "localhost",
                                        },
                                    ],
                                }
                            ],
                        },
                    },
                },
            }

        mock_custom_objects_api.return_value.get_namespaced_custom_object.side_effect = (
            mock_get_namespaced_custom_object
        )

        state = self.composer._deploy_stats_collector()

        mock_core_api.create_namespaced_pod.assert_called_with(
            body={
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "stats-collector",
                    "namespace": "composer-3-0-0-airflow-2-6-3-aaabbb",
                },
                "spec": {
                    "runtimeClassName": "gke-node",
                    "containers": [
                        {
                            "name": "stats-collector",
                            "image": "image-worker-scheduler",
                            "args": ["stats-collector"],
                            "env": [
                                {
                                    "name": "ENV1",
                                    "value": "VALUE1",
                                },
                                {
                                    "name": "SQL_HOST",
                                    "value": "airflow-sqlproxy-service.composer-system.svc.cluster.local",
                                },
                            ],
                        }
                    ],
                },
            },
            namespace="composer-3-0-0-airflow-2-6-3-aaabbb",
        )
        self.assertEqual(state, State.UPLOAD_DAG)

    def test_deploy_stats_collector_unsupported_composer_version(self):
        self.composer.config_body = {
            "config": {
                "softwareConfig": {
                    "imageVersion": "composer-1.20.1-airflow-1.10.15",
                },
            },
        }

        with self.assertRaises(ValueError):
            state = self.composer._deploy_stats_collector()

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    def test_reset_environment(self, mock_check_cluster_readiness):

        runner_mock = mock.MagicMock()
        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )
        self.composer.remote_runner_provider = remote_runner_provider_mock
        self.composer.state = State.RESET_ENV

        state = self.composer._reset_environment()

        mock_check_cluster_readiness.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            container=STATS_COLLECTOR_CONTAINER_NAME,
            pod_prefix=STATS_COLLECTOR_POD_PREFIX,
        )
        runner_mock.reset_airflow_database.assert_called_once_with()

        self.assertEqual(state, State.UPLOAD_DAG)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=False)
    def test_reset_environment__cluster_not_ready(self, mock_check_cluster_readiness):

        runner_mock = mock.MagicMock()
        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )
        self.composer.remote_runner_provider = remote_runner_provider_mock
        self.composer.state = State.RESET_ENV

        state = self.composer._reset_environment()

        mock_check_cluster_readiness.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_not_called()
        runner_mock.reset_airflow_database.assert_not_called()

        self.assertEqual(state, State.RESET_ENV)

    @mock.patch(
        MODULE_NAME + ".ComposerEnvironment.get_bucket_name_and_dags_folder",
        return_value=(BUCKET_NAME, DAGS_FOLDER),
    )
    @mock.patch(MODULE_NAME + ".generate_copies_of_elastic_dag")
    def test_upload_dag_files(
        self, mock_generate_copies_of_elastic_dag, mock_get_bucket_name_and_dags_folder
    ):

        self.composer.state = State.UPLOAD_DAG

        mock_generate_copies_of_elastic_dag.return_value.__enter__.return_value = (
            "tmp_dir_path",
            [DAG_FILE_PATH],
        )

        state = self.composer.upload_dag_files()

        mock_get_bucket_name_and_dags_folder.assert_called_once_with()

        mock_generate_copies_of_elastic_dag.assert_called_once_with(DAG_FILE_PATH, CONFIG_BODY_ENV_VARS)
        self.composer.storage_client.upload_dag_files.assert_called_once_with(
            dag_file_paths=[DAG_FILE_PATH],
            bucket_name=BUCKET_NAME,
            dags_folder=DAGS_FOLDER,
        )
        self.assertEqual(state, State.WAIT_FOR_DAG)

    def test_get_bucket_name_and_dags_folder(self):

        bucket_name, dags_folder = self.composer.get_bucket_name_and_dags_folder()
        self.assertEqual(bucket_name, BUCKET_NAME)
        self.assertEqual(dags_folder, DAGS_FOLDER)

    def test_check_if_drs_enabled(self):
        pass

    def test_get_results_object_name_components(self):
        test_id = uuid.uuid4()

        df = pd.DataFrame(
            {
                "uuid": [test_id],
                "environment_name": ["test_environment_name"],
                "environment_type": ["env_type"],
                "composer_version": ["1.10.3"],
                "airflow_version": ["1.10.6"],
                "python_version": ["2.7.12"],
                "test_start_date": ["1970-01-01 20:30:45.123456"],
            }
        )

        expected_components = (
            "19700101",
            "test_environment_name",
            "env_type",
            "1.10.3",
            "AIRFLOW",
            "1.10.6",
            "PY",
            "2.7.12",
            str(test_id),
        )

        result = self.composer.get_results_object_name_components(results_df=df)
        self.assertEqual(result, expected_components)
