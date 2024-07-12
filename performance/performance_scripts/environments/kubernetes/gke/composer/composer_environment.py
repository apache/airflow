"""
Classes used for communication with Google Cloud Composer API
See: http://googleapis.github.io/google-api-python-client/docs/dyn/composer_v1.html
"""

import enum
import functools
import logging
import re
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import shortuuid
from googleapiclient.discovery import build
from kubernetes.client import CustomObjectsApi, models as k8s
from pandas import DataFrame

from performance_scripts.environments.base_environment import Action, State, is_state
from performance_scripts.environments.kubernetes.gke.gke_based_environment import (
    GKEBasedEnvironment,
    handle_reconciling_cluster,
)
from performance_scripts.performance_dags.elastic_dag.elastic_dag_utils import (
    add_perf_start_date_env_to_conf,
    generate_copies_of_elastic_dag,
    validate_elastic_dag_conf,
)
from performance_scripts.utils.file_utils import (
    read_json_file,
    read_templated_json_file,
)
from performance_scripts.utils.google_cloud.storage_client import StorageClient

API_BETA_VERSION = "v1beta1"
DEFAULT_API_VERSION = "v1"
ENVIRONMENT_TYPE = "COMPOSER"
COMPOSER2_SYSTEM_NAMESPACE = "composer-system"
COMPOSER2_USER_WORKLOADS_NAMESPACE = "composer-user-workloads"
COMPOSER3_WORKLOADS_NAMESPACE = "composer-workloads"
ENVIRONMENT_ID_REGEX = "^[a-z](?:[-0-9a-z]{0,62}[0-9a-z])?$"
LISTING_ENVIRONMENTS_PAGE_SIZE = 20
STATS_COLLECTOR_POD_PREFIX = "stats-collector"
STATS_COLLECTOR_CONTAINER_NAME = "stats-collector"

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class ComposerInstanceState(enum.Enum):
    """
    Class representing possible states of Composer environment instance.
    """

    CREATING = "CREATING"
    DELETING = "DELETING"
    ERROR = "ERROR"
    RUNNING = "RUNNING"
    STATE_UNSPECIFIED = "STATE_UNSPECIFIED"
    UPDATING = "UPDATING"


# pylint: disable=too-many-public-methods
# pylint: disable=too-many-lines
class ComposerEnvironment(GKEBasedEnvironment):
    # pylint: disable=too-many-instance-attributes
    """
    Class representing an instance of Cloud Composer environment.
    """

    environment_type = ENVIRONMENT_TYPE

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        instance_specification_file_path: str,
        elastic_dag_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
        results_columns: Optional[Dict] = None,
        reuse_if_exists: bool = False,
        delete_if_exists: bool = False,
        delete_upon_finish: bool = False,
        reset_environment: bool = False,
    ) -> None:
        """
        Creates an instance of ComposerEnvironment.

        :param instance_specification_file_path: path to the json file with specification
            of Composer environment. Contents of this json file will be used in a request creating
            the environment instance.
        :type instance_specification_file_path: str
        :param elastic_dag_path: a path to elastic DAG file copies of which should be uploaded
            to the storage of given Composer instance.
        :type elastic_dag_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from instance_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]
        :param results_columns: an optional dictionary with additional column name/value pairs
            that should be added to the results table.
        :type results_columns: Dict
        :param reuse_if_exists: set to True if you want to reuse an existing environment with
            the same name. Note that the actual configuration might differ from the one specified
            in configuration file. This flag takes precedence over delete_if_exists.
        :type reuse_if_exists: bool
        :param delete_if_exists: set to True if you want an existing environment with the same name
            to be deleted and then recreated.
        :type delete_if_exists: bool
        :param delete_upon_finish: set to True if you want the environment to be deleted in case
            of an error.
        :type delete_upon_finish: bool
        :param reset_environment: set to True if you want Airflow database to be purged before
            reusing an existing environment. This flag is only used if reuse_if_exists is True.
        :type reset_environment: bool
        """

        super().__init__(
            namespace_prefix=None,
            pod_prefix=STATS_COLLECTOR_POD_PREFIX,
            container_name=STATS_COLLECTOR_CONTAINER_NAME,
            system_namespaces=[
                COMPOSER2_SYSTEM_NAMESPACE,
                COMPOSER2_USER_WORKLOADS_NAMESPACE,
                COMPOSER3_WORKLOADS_NAMESPACE,
            ],
        )

        self.elastic_dag_path = elastic_dag_path
        self.results_columns = results_columns
        self.reuse_if_exists = reuse_if_exists
        self.delete_if_exists = delete_if_exists
        self.delete_upon_finish = delete_upon_finish
        self.reset_environment = reset_environment

        jinja_variables_dict = jinja_variables_dict or {}

        (
            environment_specification,
            self.project_id,
            self.location_id,
            self.environment_id,
        ) = self.load_specification(
            instance_specification_file_path,
            elastic_dag_config_file_path,
            jinja_variables_dict,
        )

        # TODO: move this to a function?
        self.config_body = environment_specification["config_body"]
        api_version = environment_specification.get("api_version", DEFAULT_API_VERSION)
        api_endpoint = environment_specification.get("api_endpoint", None)
        self.force_routing = environment_specification.get("force_routing", True)

        self.composer_api = ComposerApi(api_version, api_endpoint)
        self.storage_client = StorageClient(project_id=self.get_project_id())

        # pylint: enable=too-many-instance-attributes
        # pylint: enable=too-many-arguments

    @staticmethod
    def get_name_form() -> str:
        """
        Returns the form which the full name of Composer Environment should follow.
        """
        return "projects/{project_id}/locations/{location_id}/environments/{environment_id}"

    @staticmethod
    def get_default_jinja_variables_values() -> Dict[str, str]:
        """
        Returns the dictionary containing default values of some configuration options that
        can be used as values for jinja variables when rendering specification file.
        """
        return {
            # use v1beta1 by default as it contains features in preview stage
            "api_version": "v1beta1",
            "api_endpoint": "",
            "force_routing": "true",
            "python_version": "3",
            "composer_version": "1",
            "airflow_version": "1.10",
            "airflow_config_overrides": "{}",
            "cloud_data_lineage_integration": "{}",
            "disk_size_gb": "100",
            "machine_type": "n1-standard-2",
            "network_id": "default",
            "subnetwork_id": "default",
            "use_ip_aliases": "false",
            "node_count": "3",
            # this is hack to force Composer API resolve scheduler_count field to default value
            "scheduler_count": "0",
            "enable_private_environment": "false",
            "enable_private_endpoint": "false",
            "master_ipv4_cidr_block": "",  # this setting is correct for public IP env
            "scheduler_resource_config": "{}",
            "webserver_resource_config": "{}",
            "worker_resource_config": "{}",
            "triggerer_resource_config": "{}",
            "dag_processor_resource_config": "{}",
        }

    @staticmethod
    def construct_environment_id(jinja_variables_dict: Dict[str, str]) -> str:
        """
        Creates environment id based on the contents of the jinja variables dict.
        Note that the returned value will not necessarily contain true information
        about the environment and it may even not be used at all - it depends on the
        specification file contents.

        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain composer_version,
            airflow_version, python_version and node_count keys.
        :type jinja_variables_dict: Dict[str, str]

        :return: returned environment id
        :rtype: str
        """

        # TODO: improve this so it always uses true information from rendered file

        environment_id = "-".join(
            [
                "cmp-{composer_version}",
                "af{airflow_version}",
                "py{python_version}",
                "{node_count}nodes",
            ]
        ).format(**jinja_variables_dict)

        return environment_id.lower().replace(".", "-")[:64].strip("-")

    @staticmethod
    def update_config_body_with_elastic_dag_env_variables(
        config_body: Dict, elastic_dag_conf_path: str
    ) -> None:
        """
        Reads the json file under elastic_dag_conf_path
        and updates config_body with environment variables stored in it

        :param config_body: dictionary with configuration of Composer instance.
        :type config_body: Dict
        :param elastic_dag_conf_path: path to the file with configuration for elastic DAG.
        :type elastic_dag_conf_path: str

        :raises:
            TypeError: if configuration file is not a dictionary.
        """

        elastic_dag_conf = read_json_file(elastic_dag_conf_path)

        if not isinstance(elastic_dag_conf, Dict):
            raise TypeError("Elastic dag configuration is expected to be a dictionary.")

        # after executing check_and_adjust_config_body
        # envVariables is expected to exist in configuration
        config_body["config"]["softwareConfig"]["envVariables"].update(elastic_dag_conf)

    @staticmethod
    def validate_environment_id(environment_id: str) -> None:
        """
        Checks provided string to determine
        if it fulfills the requirement for Composer environment id.

        :param environment_id: string to check.
        :type environment_id: str

        :raises:
            TypeError: if environment_id does not meet the requirements.
        """
        match = re.compile(ENVIRONMENT_ID_REGEX).match(environment_id)

        if match is None:
            raise ValueError(
                f"Environment id '{environment_id}' " f"does not match the regex: {ENVIRONMENT_ID_REGEX}"
            )

    @staticmethod
    def get_random_environment_id(environment_id: str) -> str:
        """
        Adds a random part to the provided string to create a new Composer environment id.
        The method assumes that environment_id is already a valid id.
        """
        random_part = shortuuid.ShortUUID().random(length=10).lower()
        return f"{environment_id[:(64 - len(random_part) - 1)]}-{random_part}"

    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments
    @classmethod
    def prepare_specifications_for_multiple_test_attempts(
        cls,
        number_of_copies: int,
        instance_specification_file_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
        randomize_environment_name: bool = True,
    ) -> Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]:
        """
        Reads provided specification file, renders it and prepares a provided number of copies of
        resulting specification.

        :param number_of_copies: number of copies of the specification that should be prepared.
        :type number_of_copies: int
        :param instance_specification_file_path: path to the file with specification
            of Composer environment.
        :type instance_specification_file_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from instance_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]
        :param randomize_environment_name: if set to True, then a random part will be added to
            environment name of every specification copy.
        :type randomize_environment_name: bool

        :return: a tuple of two lists. Both lists contain pairs consisting of specification dicts
            and environment ids:
            - first list contains specifications that can be run in parallel
            - second list contains specifications that should be run sequentially
        :rtype: Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]

        :raises: any Exception that was raised during execution of load_specification method
        """

        specifications_to_run_in_parallel = []
        specifications_to_run_in_sequence = []

        try:
            (
                environment_specification,
                project_id,
                location_id,
                environment_id,
            ) = cls.load_specification(
                instance_specification_file_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
            )
        except Exception as exception:
            log.error(
                "Error occurred when reading environment specification file: %s\n"
                "Provided elastic dag config file: %s\n"
                "Provided jinja_variables_dict: %s",
                instance_specification_file_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
            )
            raise exception

        private = (
            environment_specification["config_body"]["config"]
            .get("privateEnvironmentConfig", {})
            .get("enablePrivateEnvironment", False)
        )

        for _ in range(number_of_copies):
            new_environment_specification = deepcopy(environment_specification)
            new_environment_id = (
                cls.get_random_environment_id(environment_id)
                if randomize_environment_name
                else environment_id
            )
            new_environment_specification["config_body"]["name"] = cls.fill_name_form(
                project_id, location_id, new_environment_id
            )
            # TODO: need better check that would involve IP ranges and networks
            if private:
                specifications_to_run_in_sequence.append((new_environment_specification, new_environment_id))
            else:
                specifications_to_run_in_parallel.append((new_environment_specification, new_environment_id))

        return specifications_to_run_in_parallel, specifications_to_run_in_sequence

    # pylint: enable=too-many-locals
    # pylint: enable=too-many-arguments

    @classmethod
    def load_specification(
        cls,
        instance_specification_file_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
    ) -> Tuple[Dict, str, str, str]:
        """
        Loads the files under specified paths, validates their contents and returns Composer
        environment specification and other environment related values.

        :param instance_specification_file_path: path to the file with specification
            of Composer environment.
        :type instance_specification_file_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from instance_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]

        :return: a tuple consisting of:
            - a dictionary with specification of Composer environment
            - Google Cloud project id of Composer environment
            - location of Composer environment
            - name of Composer environment
        :rtype: Tuple[Dict, str, str, str]

        :raises:
            TypeError: if the environment specification is not a dictionary.
            KeyError: if the specification does not contain 'config_body' key.
        """

        jinja_variables_dict = jinja_variables_dict or {}

        jinja_variables_to_use = {
            **cls.get_default_jinja_variables_values(),
            **jinja_variables_dict,
        }

        if "environment_id" not in jinja_variables_to_use:
            jinja_variables_to_use["environment_id"] = cls.construct_environment_id(jinja_variables_to_use)

        environment_specification = read_templated_json_file(
            instance_specification_file_path, jinja_variables_to_use
        )

        if not isinstance(environment_specification, Dict):
            raise TypeError("Environment specification is expected to be a dictionary.")

        if "config_body" not in environment_specification:
            raise KeyError(
                "Specification of Composer environment must contain 'config_body' key "
                "with configuration of Composer instance."
            )

        # base check of config body
        cls.check_and_adjust_config_body(environment_specification["config_body"])

        # elastic dag part of validation
        if elastic_dag_config_file_path is not None:
            cls.update_config_body_with_elastic_dag_env_variables(
                environment_specification["config_body"], elastic_dag_config_file_path
            )

        add_perf_start_date_env_to_conf(
            environment_specification["config_body"]["config"]["softwareConfig"]["envVariables"]
        )
        validate_elastic_dag_conf(
            environment_specification["config_body"]["config"]["softwareConfig"]["envVariables"]
        )

        # collect and check the environment id
        project_id, location_id, environment_id = cls.retrieve_ids_from_name(
            environment_specification["config_body"]["name"]
        )

        return environment_specification, project_id, location_id, environment_id

    @classmethod
    def check_and_adjust_config_body(cls, config_body: Dict) -> None:
        """
        Validates the contents of given dictionary and makes optional changes aimed at
        using it to create an instance of Composer environment.

        :param config_body: dictionary with configuration of Composer instance.
        :type config_body: Dict

        :raises: TypeError or KeyError if the configuration does not meet the requirements.
        """

        if not isinstance(config_body, Dict):
            raise TypeError("Composer configuration is expected to be a dictionary.")

        if "name" not in config_body:
            raise KeyError(
                f"Composer configuration must contain 'name' key with environment name in form: "
                f"'{cls.get_name_form()}'"
            )

        if "config" not in config_body:
            raise KeyError("Composer configuration must contain 'config' key.")

        if not isinstance(config_body["config"], Dict):
            raise TypeError("In composer configuration a dictionary is expected " "under 'config' key.")

        if "softwareConfig" not in config_body["config"]:
            raise KeyError(
                "In composer configuration a dictionary under 'config' key "
                "must contain 'softwareConfig' key."
            )

        if not isinstance(config_body["config"]["softwareConfig"], Dict):
            raise TypeError(
                "In composer configuration a dictionary is expected " "under 'config' -> 'softwareConfig'."
            )

        if "envVariables" not in config_body["config"]["softwareConfig"]:
            config_body["config"]["softwareConfig"]["envVariables"] = {}

        if not isinstance(config_body["config"]["softwareConfig"]["envVariables"], Dict):
            raise TypeError(
                "In composer configuration a dictionary is expected "
                "under 'config' -> 'softwareConfig' -> 'envVariables'."
            )

    @classmethod
    def retrieve_ids_from_name(cls, name: str) -> Tuple[str, str, str]:
        """
        Retrieves project id, location and environment id from the Composer environment name.
        Also validates the correctness of environment id.

        :param name: full name of Composer instance.
        :type name: str

        :return: tuple of three string - project_id, location and environment_id.
        :rtype: Tuple[str, str, str]

        :raises: ValueError: if the values could not be retrieved from the name.
        """

        try:
            _, project_id, _, location_id, _, environment_id = name.split("/")
        except ValueError as exception:
            log.error(
                "Error occurred when retrieving ids from environment name: %s. Make sure the "
                "name follows form: '%s'.",
                name,
                cls.get_name_form(),
            )
            raise exception

        cls.validate_environment_id(environment_id)

        return project_id, location_id, environment_id

    @classmethod
    def fill_name_form(cls, project_id: str, location_id: str, environment_id: str) -> str:
        """
        Returns full name of the Composer environment, filling the template with the provided ids.
        """

        return cls.get_name_form().format(
            project_id=project_id,
            location_id=location_id,
            environment_id=environment_id,
        )

    @property
    def name(self) -> str:
        """
        Returns the name of given Composer instance.
        """
        return self.config_body["name"]

    @property
    def states_map(self) -> Dict[State, Action]:
        """
        Returns a map containing a method that should be executed for every applicable state
        to move the performance test forward.
        """
        return {
            State.NONE: Action(self.prepare_environment, sleep_time=None, retryable=True),
            State.WAIT_UNTIL_READY: Action(self.is_composer_instance_ready, sleep_time=60.0, retryable=True),
            State.WAIT_UNTIL_CAN_BE_DELETED: Action(
                self.is_composer_instance_ready, sleep_time=60.0, retryable=True
            ),
            State.DELETING_ENV: Action(self._wait_for_deletion, sleep_time=30.0, retryable=True),
            State.UPDATE_ENV_INFO: Action(self._update_environment_info, sleep_time=10.0, retryable=True),
            State.DEPLOY_STATS_COLLECTOR: Action(
                self._deploy_stats_collector, sleep_time=10.0, retryable=True
            ),
            State.RESET_ENV: Action(self._reset_environment, sleep_time=10.0, retryable=True),
            State.UPLOAD_DAG: Action(self.upload_dag_files, sleep_time=10.0, retryable=True),
            State.WAIT_FOR_DAG: Action(self.check_if_dags_have_loaded, sleep_time=30.0, retryable=True),
            State.UNPAUSE_DAG: Action(self.unpause_dags, sleep_time=10.0, retryable=True),
            State.WAIT_FOR_DAG_RUN_EXEC: Action(
                self.check_dag_run_execution_status, sleep_time=60.0, retryable=True
            ),
            State.COLLECT_RESULTS: Action(self.collect_results, sleep_time=10.0, retryable=True),
        }

    @property
    def is_terminal_state_and_deletable(self) -> bool:
        """
        Returns True if environment is in one of its terminal states and it is
        possible to delete it.
        """
        if not self.check_if_exists():
            return False
        composer_instance_state = self.get_state()
        return self.state in self.terminal_states_deletable and is_state(
            composer_instance_state,
            (ComposerInstanceState.RUNNING, ComposerInstanceState.ERROR),
        )

    def prepare_environment(self) -> State:
        """
        Prepares Composer environment for performance tests - either creating, recreating or reusing
        the existing one, depending on the flags set.
        """
        # Check if composer instance already exists
        if not self.check_if_exists():
            log.info("Creating environment %s.", self.name)
            self.composer_api.create_environment(self.get_parent(), self.config_body)
            return State.WAIT_UNTIL_READY

        composer_instance_state = self.get_state()

        if is_state(composer_instance_state, ComposerInstanceState.DELETING):
            log.info(
                "Environment with name: %s already exists but is currently being deleted. "
                "Waiting for deletion to finish before recreation.",
                self.name,
            )
            return State.DELETING_ENV

        if self.reuse_if_exists:
            log.info(
                "Environment with name: '%s' already exists. Reusing it as reuse-if-exists " "flag was set.",
                self.name,
            )
            return State.WAIT_UNTIL_READY

        if self.delete_if_exists:
            log.info(
                "Environment with name: '%s' already exists. Recreating it as delete-if-exists "
                "flag was set.",
                self.name,
            )
            return State.WAIT_UNTIL_CAN_BE_DELETED

        log.error(
            "Environment with name: %s already exists and neither delete-if-exists "
            "nor reuse-if-exists flag was set. Exiting.",
            self.name,
        )
        return State.FAILED_DO_NOT_DELETE

    def is_composer_instance_ready(self) -> State:
        """
        Checks if the process of creation/updating Composer instance has finished.
        """
        composer_instance_state = self.get_state()
        not_ready_states = (
            ComposerInstanceState.CREATING,
            ComposerInstanceState.UPDATING,
        )
        # we are still waiting, do not change the current state
        if is_state(composer_instance_state, not_ready_states):
            return self.state

        # the environment can be used now
        if (
            is_state(composer_instance_state, ComposerInstanceState.RUNNING)
            and self.state == State.WAIT_UNTIL_READY
        ):
            return State.UPDATE_ENV_INFO

        # the environment can be deleted now
        if (
            is_state(
                composer_instance_state,
                (ComposerInstanceState.ERROR, ComposerInstanceState.RUNNING),
            )
            and self.state == State.WAIT_UNTIL_CAN_BE_DELETED
        ):
            self.delete_environment()
            return State.DELETING_ENV

        log.error(
            "An error occurred - environment %s has reached '%s' state. "
            "Please, check this environment in Cloud Console.",
            self.name,
            composer_instance_state,
        )
        return State.FAILED

    def _wait_for_deletion(self) -> State:
        """
        Waits until given environment ceases to exist completely.
        """
        if not self.check_if_exists():
            log.info("Environment %s was deleted.", self.name)
            return State.NONE
        return self.state

    def check_if_exists(self) -> bool:
        """
        Checks if Composer environment with given name already exists.

        :return: True if environment exists (regardless of its state) and False otherwise.
        :rtype: bool
        """
        parent = self.get_parent()
        name = self.name
        page_token = None

        while True:
            response = self.composer_api.list_environments(
                parent=parent,
                page_size=LISTING_ENVIRONMENTS_PAGE_SIZE,
                page_token=page_token,
            )

            if any(e["name"] == name for e in response.get("environments", [])):
                return True

            if "nextPageToken" not in response:
                return False

            page_token = response["nextPageToken"]

    def get_state(self) -> str:
        """
        Returns the current state of given Composer environment.

        :return: string with current state of the environment.
        :rtype: str
        """

        return self.composer_api.get_environment(self.name)["state"]

    def delete_environment(self) -> None:
        """
        Deletes the environment. This function is unsafe - it expects the proper conditions
        for environment deletion have been met: the environment exists and is either in ERROR or
        RUNNING state.
        """
        log.info("Deleting the environment %s.", self.name)
        self.composer_api.delete_environment(self.name)

    def _update_environment_info(self) -> State:
        """
        Updates config_body with the response from get_environment endpoint that contains
        information available only upon environment creation (like airflow URI or GKE cluster
        details). This way some information that are not supposed to change for an existing
        environment can be retrieved from config_body instead of making get_environment request
        every time. Also updates remote runner provider and adjusts the environment that is
        reused for performance test purposes.
        """

        # update config in env_info with response from get request
        ready_env_config = self.composer_api.get_environment(self.name)

        if self.reuse_if_exists:
            self.check_and_adjust_config_body(ready_env_config)
            validate_elastic_dag_conf(ready_env_config["config"]["softwareConfig"]["envVariables"])

        self.config_body = ready_env_config

        # use routing only if the public endpoint is not accessible OR the cluster is private and
        # you have forced routing
        use_routing = False
        if self.check_if_private_ip_enabled() and self.force_routing:
            use_routing = True
        elif not self.check_if_public_endpoint_enabled():
            use_routing = True

        if self.get_composer_major_version() < 3:
            # In Composer 1/2, versioned namespace is prefixed with "composer".
            self.namespace_prefix = "composer"
        else:
            self.namespace_prefix = "airflow"

        # create an instance of kubernetes api using GKE cluster details
        self.update_remote_runner_provider(
            project_id=self.get_gke_project_id(),
            zone=self.get_zone(),
            cluster_id=self.get_gke_cluster_id(),
            use_routing=use_routing,
        )

        log.info(
            "Environment %s is ready. Logs are available under: %s",
            self.name,
            self.get_stackdriver_logs_uri(),
        )

        if self.reuse_if_exists and self.reset_environment:
            return State.RESET_ENV

        return State.DEPLOY_STATS_COLLECTOR

    def _deploy_stats_collector(self) -> State:
        """Deploys stats-collector workload to GKE cluster."""
        log.info("Deploying stats collector.")

        composer_major_version = self.get_composer_version().split(".")[0]
        if composer_major_version not in ["2", "3"]:
            raise ValueError(f"Unsupported Composer major version: {composer_major_version}")

        with self.remote_runner_provider.get_kubernetes_apis_in_isolated_context() as (
            core_api,
            apps_api,
            _,
        ):
            api_client = core_api.api_client
            custom_objects_api = CustomObjectsApi(api_client=api_client)

            versioned_namespace = self.remote_runner_provider.find_namespace_with_a_prefix(
                core_api, self.namespace_prefix, self.system_namespaces
            )
            log.info("Found namespace: %s.", versioned_namespace)

            # TODO: extract to constants
            worker_template = custom_objects_api.get_namespaced_custom_object(
                group="composer.cloud.google.com",
                version=API_BETA_VERSION,
                plural="airflowworkersets",
                name="airflow-worker",
                namespace=versioned_namespace,
            )["spec"]["template"]
            for container in worker_template["spec"]["containers"]:
                if container["name"] == "airflow-worker":
                    airflow_worker_container = container
                    break
            else:
                raise ValueError("Airflow worker container was not found")

            # Define pod spec, take into account Composer 2, 3 differences.
            pod = k8s.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=k8s.V1ObjectMeta(
                    name="stats-collector",
                    namespace=versioned_namespace,
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="stats-collector",
                            image=airflow_worker_container["image"],
                            args=["stats-collector"],
                            env=airflow_worker_container["env"],
                        )
                    ]
                ),
            )
            if composer_major_version == "2":
                pod.spec.volumes = [
                    k8s.V1Volume(
                        name="airflow-config",
                        config_map=k8s.V1ConfigMapVolumeSource(name="airflow-configmap"),
                    )
                ]
                pod.spec.containers[0].volume_mounts = [
                    k8s.V1VolumeMount(name="airflow-config", mount_path="/etc/airflow/airflow_cfg")
                ]
            if composer_major_version == "3":
                pod.spec.runtime_class_name = "gke-node"
                for env_var in pod.spec.containers[0].env:
                    if env_var["name"] == "SQL_HOST":
                        env_var["value"] = "airflow-sqlproxy-service.composer-system.svc.cluster.local"
                        break
            sanitized_pod = api_client.sanitize_for_serialization(pod)
            core_api.create_namespaced_pod(body=sanitized_pod, namespace=versioned_namespace)

        return State.UPLOAD_DAG

    @handle_reconciling_cluster
    def _reset_environment(self) -> State:
        """
        Resets the environment, provided that specific flags were set.
        """

        # TODO: additional steps to fully reset the environment:
        #  - remove all the dag files other than airflow_monitoring
        #    and wait until they disappear from Dag Bags on pods
        #  - clean logs
        #  - clean redis queue
        #  - restart scheduler and worker pod
        #  - patch environment with new configuration for elastic dag

        log.info("Resetting Airflow metadata database on environment %s.", self.name)

        # reset Airflow database to make sure the Dag Runs from previous execution are no
        # longer present
        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            runner.reset_airflow_database()

        return State.UPLOAD_DAG

    def upload_dag_files(self) -> State:
        """
        Uploads dag files to the bucket dedicated to test environment.
        """

        bucket_name, dags_folder = self.get_bucket_name_and_dags_folder()

        log.info("Uploading dag files to environment %s.", self.name)

        with generate_copies_of_elastic_dag(self.elastic_dag_path, self.get_env_variables()) as (
            _,
            elastic_dag_copies,
        ):
            self.storage_client.upload_dag_files(
                dag_file_paths=elastic_dag_copies,
                bucket_name=bucket_name,
                dags_folder=dags_folder,
            )

        return State.WAIT_FOR_DAG

    def get_bucket_name_and_dags_folder(self) -> Tuple[str, str]:
        """
        Gets the location where DAG files are stored on given Composer instance
        and splits it into bucket and folder path

        :return: name of the bucket and folder where dag files are stored
        :rtype: Tuple[str, str]
        """

        dag_gcs_prefix = self.config_body["config"]["dagGcsPrefix"]

        bucket_name = dag_gcs_prefix.split("/")[2]

        dags_folder = "/".join(dag_gcs_prefix.split("/")[3:])

        return bucket_name, dags_folder

    def prepare_environment_columns(self) -> OrderedDict:
        """
        Prepares an OrderedDict containing environment information that will serve as columns
        for the results dataframe.

        :return: a dict with a subset of Composer configuration options
            in order in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        environment_columns = OrderedDict(
            [
                ("environment_name", self.environment_id),
                ("environment_type", self.environment_type),
                ("composer_version", self.get_composer_version()),
                ("airflow_version", self.get_airflow_version()),
                ("python_version", self.collect_python_version()),
                ("gke_version", self.get_gke_version()),
                ("composer_api_version", self.composer_api.get_version()),
                ("composer_api_endpoint", self.composer_api.get_api_endpoint()),
                ("environment_size", self.get_environment_size()),
                ("node_count", self.get_node_count()),
                ("scheduler_count", self.get_scheduler_count()),
                ("machine_type", self.get_machine_type()),
                ("disk_size_gb", self.get_disk_size()),
                ("private_ip_enabled", self.check_if_private_ip_enabled()),
                ("drs_enabled", self.check_if_drs_enabled()),
            ]
        )
        return environment_columns

    def get_parent(self) -> str:
        """
        Returns the string with project and location information regarding given Composer instance.
        """
        return f"projects/{self.project_id}/locations/{self.location_id}"

    def get_project_id(self) -> str:
        """
        Returns the project_id of given Composer instance.
        """
        return self.project_id

    def get_gke_cluster_name(self) -> str:
        """
        Returns full GKE cluster address.
        """
        return self.config_body["config"]["gkeCluster"]

    def get_gke_project_id(self) -> str:
        """
        Extracts project id part from GKE cluster address
        """
        return self.config_body["config"]["gkeCluster"].split("/")[1]

    def get_zone(self) -> str:
        """
        Extracts zone part from GKE cluster address
        """
        return self.config_body["config"]["gkeCluster"].split("/")[3]

    def get_gke_cluster_id(self) -> str:
        """
        Extracts cluster id part from GKE cluster address
        """
        return self.config_body["config"]["gkeCluster"].split("/")[5]

    def get_env_variables(self) -> Dict[str, str]:
        """
        Returns dictionary with environment variables set on given Composer instance.

        :return: dict with environment variables as keys and their values as values.
        :rtype: Dict[str, str]
        """
        return self.config_body["config"]["softwareConfig"]["envVariables"]

    def get_composer_version(self) -> str:
        """
        Extracts version of composer-addon from image version.
        """
        return (
            self.config_body["config"]["softwareConfig"]["imageVersion"]
            .split("composer-")[1]
            .split("-airflow")[0]
        )

    def get_composer_major_version(self) -> int:
        """
        Returns major version of composer-addon from image version.
        """
        return int(self.get_composer_version().split(".")[0])

    def get_airflow_version(self) -> str:
        """
        Extracts version of Apache Airflow from image version.
        """
        return self.config_body["config"]["softwareConfig"]["imageVersion"].split("airflow-")[1]

    def get_node_count(self) -> int:
        """
        Returns number of GKE cluster's node VMs in this instance of Cloud Composer.
        """
        return self.config_body["config"].get("nodeCount")

    def get_scheduler_count(self) -> int:
        """
        Returns number of schedulers.
        """
        with self.remote_runner_provider.get_kubernetes_apis_in_isolated_context() as (
            core_api,
            apps_api,
            _,
        ):
            namespace = self.remote_runner_provider.find_namespace_with_a_prefix(
                core_api, self.namespace_prefix, self.system_namespaces
            )
            log.info("Found namespace: %s.", namespace)
            response = apps_api.list_namespaced_deployment(namespace)
            for item in response.to_dict()["items"]:
                if item["metadata"]["name"] == "airflow-scheduler":
                    return item["spec"]["replicas"]
        return None

    def get_machine_type(self) -> str:
        """
        Returns type of machine used as GKE cluster's node VMs in this instance of Cloud Composer.
        """
        return self.config_body["config"]["nodeConfig"].get("machineType", "").split("/")[-1]

    def get_disk_size(self) -> int:
        """
        Returns the disk size in GB used for node VMs in this instance of Cloud Composer.
        """
        return self.config_body["config"]["nodeConfig"].get("diskSizeGb")

    def check_if_private_ip_enabled(self) -> bool:
        """
        Returns True if this instance of Cloud Composer uses Private IP and False otherwise.
        """
        return self.config_body["config"]["privateEnvironmentConfig"].get("enablePrivateEnvironment", False)

    def check_if_public_endpoint_enabled(self) -> bool:
        """
        Returns True if this instance of Cloud Composer has access to public master
        endpoint and False otherwise.
        """
        private_cluster_config = self.config_body["config"]["privateEnvironmentConfig"].get(
            "privateClusterConfig", {}
        )
        return not private_cluster_config.get("enablePrivateEndpoint", False)

    def check_if_drs_enabled(self) -> bool:
        """
        Returns True if this instance has DRS enabled and False otherwise.
        """
        with self.remote_runner_provider.get_kubernetes_apis_in_isolated_context() as (
            _,
            apps_api,
            _,
        ):
            deployment = self.remote_runner_provider.get_deployment(apps_api, "gcs-syncd", "default")
        return deployment is not None

    def get_airflow_uri(self) -> str:
        """
        Gets the uri of Airflow webserver
        """
        return self.config_body["config"]["airflowUri"]

    def get_stackdriver_logs_uri(self) -> str:
        """
        Gets the uri of Stackdriver logs.
        """

        return (
            f"https://console.cloud.google.com/logs/viewer?resource=cloud_composer_environment/"
            f"location/{self.location_id}/"
            f"environment_name/{self.environment_id}"
            f"&project={self.project_id}"
        )

    def get_results_object_name_components(self, results_df: DataFrame) -> Sequence[str]:
        """
        Gets the sequence of components for the results object's name (file, GCS blob, BQ table)
        based on contents of the results dataframe.

        :param results_df: pandas Dataframe containing information about tested environment
            configuration and performance metrics.
        :type results_df: DataFrame

        :return: a sequence of strings which should be combined to form the name
            of the results object.
        :rtype: Sequence[str]
        """

        # TODO: this method should not depend on contents of results_df, all required info should
        #  be collected using class methods
        test_start_day = datetime.strptime(results_df["test_start_date"][0], "%Y-%m-%d %H:%M:%S.%f").strftime(
            "%Y%m%d"
        )
        environment_name = results_df["environment_name"][0]
        environment_type = results_df["environment_type"][0]
        composer_version = results_df["composer_version"][0]
        airflow_version = results_df["airflow_version"][0]
        python_version = results_df["python_version"][0]
        test_id = str(results_df["uuid"][0])

        return (
            test_start_day,
            environment_name,
            environment_type,
            composer_version,
            "AIRFLOW",
            airflow_version,
            "PY",
            python_version,
            test_id,
        )


# pylint: enable=too-many-public-methods
def handle_broken_pipe_error(method):
    """
    Decorator for methods of ComposerApi class executing requests, that re-instantiates the service
    client and repeats the call in case a BrokenPipeError occurs, which happens after long periods
    of not using the service.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except BrokenPipeError:
            # re-instantiate the service and try once more
            self.update_service()
            return method(self, *args, **kwargs)

    return wrapper


class ComposerApi:
    """
    Class dedicated for communicating with Google Cloud Composer service.
    """

    def __init__(self, version: str = DEFAULT_API_VERSION, api_endpoint: Optional[str] = None) -> None:
        """
        Creates an instance of ComposerApi.

        :param version: version of Discovery API to use for executing requests. If you want to
            create an environment with beta version of composer-addon or use some of beta features,
            then you should choose v1beta1, otherwise use v1.
        :type version: str
        :param api_endpoint: optional argument allowing for overriding endpoint to which requests
            will be sent
        :type api_endpoint: str
        """

        self.version = version
        self.api_endpoint = api_endpoint or ""
        self.client_options = {"api_endpoint": api_endpoint} if api_endpoint else None
        self.service = build("composer", self.version, client_options=self.client_options)

    def get_version(self) -> str:
        """
        Returns version of api used to execute requests.
        """
        return self.version

    def get_api_endpoint(self) -> str:
        """
        Returns the api endpoint used to execute requests.
        """
        return self.api_endpoint

    def update_service(self) -> None:
        """
        Re-instantiates the service object.
        """
        self.service = build("composer", self.version, client_options=self.client_options)

    @handle_broken_pipe_error
    def list_environments(
        self,
        parent: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Dict:
        """
        Lists all environments existing under specified project and location.

        :param parent: string indicating project and location of environment that follows format:
            projects/{projectId}/locations/{locationId}
        :type parent: str
        :param page_size: maximum number of environments returned in response. If None, then all
            environments will be returned.
        :type page_size: int
        :param page_token: token pointing to the next page of environments, returned in previous
            list_environments response. If None, then first page will be returned.
        :type page_token: str

        :return: the dictionary with the configuration and status information about all collected
            environments, returned from the API.
        :rtype: Dict
        """

        list_environments_request = (
            self.service.projects()
            .locations()
            .environments()
            .list(parent=parent, pageSize=page_size, pageToken=page_token)
        )
        response = list_environments_request.execute()
        return response

    @handle_broken_pipe_error
    def get_environment(self, name: str) -> Dict:
        """
        Collects the environment specified by the name.

        :param name: name of the environment to get that follows the format:
            projects/{projectId}/locations/{locationId}/environments/{environmentId}
        :type name: str

        :return: the dictionary with the configuration and status information about the specified
            environment, returned from the API.
        :rtype: Dict
        """

        get_environment_request = self.service.projects().locations().environments().get(name=name)
        response = get_environment_request.execute()
        return response

    @handle_broken_pipe_error
    def create_environment(self, parent: str, body: Dict) -> Dict:
        """
        Creates the environment in given project and location using specified configuration.

        :param parent: string indicating project and location of environment that follows format:
            projects/{projectId}/locations/{locationId}
        :type parent: str
        :param body: dictionary with configuration of environment to create.
        :type body: Dict

        :return: the dictionary with the response returned from the API.
        :rtype: Dict
        """

        create_environment_request = (
            self.service.projects().locations().environments().create(parent=parent, body=body)
        )
        response = create_environment_request.execute()
        return response

    @handle_broken_pipe_error
    def delete_environment(self, name: str) -> Dict:
        """
        Deletes the environment specified by the name.

        :param name: name of the environment to delete that follows the format:
            projects/{projectId}/locations/{locationId}/environments/{environmentId}
        :type name: str

        :return: the dictionary with the response returned from the API.
        :rtype: Dict
        """

        delete_environment_request = self.service.projects().locations().environments().delete(name=name)
        response = delete_environment_request.execute()
        return response
