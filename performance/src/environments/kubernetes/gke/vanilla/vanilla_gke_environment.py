"""
Class representing Airflow environment installed directly on GKE cluster.
"""

# pylint: disable=too-many-lines
import logging
import os
import re
import textwrap
import uuid
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import docker
import google.auth
import shortuuid
from google.cloud.container_v1.types.cluster_service import Cluster
from pandas import DataFrame

from environments.base_environment import Action, State, is_state
from environments.kubernetes.gke.gke_based_environment import (
    GKEBasedEnvironment,
)
from performance_dags.elastic_dag.elastic_dag_utils import (
    add_perf_start_date_env_to_conf,
    generate_copies_of_elastic_dag,
    validate_elastic_dag_conf,
)
from utils.file_utils import (
    read_json_file,
    read_templated_json_file,
)
from utils.process_utils import execute_in_subprocess

HELM_CHART_PATH = os.path.join(
    os.path.abspath(__file__).split(__name__.replace(".", "/"))[0],  # project root
    "airflow-subrepo",
    "chart",
)
DAGS_FOLDER = "/opt/airflow/dags/"
ENVIRONMENT_TYPE = "VANILLA_GKE"
CLUSTER_ID_REGEX = "^[a-z](?:[-0-9a-z]{0,38}[0-9a-z])?$"
LISTING_ENVIRONMENTS_PAGE_SIZE = 20
AIRFLOW_NAMESPACE = "airflow"
AIRFLOW_WORKER_POD_PREFIX = "airflow-worker"
AIRFLOW_WORKER_CONTAINER_NAME = "worker"
DEFAULT_NODE_POOL = "default-pool"

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# pylint: disable=too-many-public-methods
class VanillaGKEEnvironment(GKEBasedEnvironment):
    # pylint: disable=too-many-instance-attributes
    """
    Class representing an instance of environment with Airflow installed on GKE cluster.
    """

    environment_type = ENVIRONMENT_TYPE

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        environment_specification_file_path: str,
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
        Creates an instance of VanillaGKEEnvironment.

        :param environment_specification_file_path: path to the json file with specification
            of `Vanilla GKE environment.
        :type environment_specification_file_path: str
        :param elastic_dag_path: a path to elastic DAG that should be uploaded to test environment.
        :type elastic_dag_path: str
        :param elastic_dag_config_file_path: path to file with configuration for elastic DAG.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]
        :param results_columns: an optional dictionary with additional column name/value pairs
            that should be added to the results table.
        :type results_columns: Dict
        :param reuse_if_exists: set to True if you want to reuse an existing cluster with
            the same name. Note that the actual cluster configuration might differ from the one
            specified in configuration file. This flag takes precedence over delete_if_exists.
        :type reuse_if_exists: bool
        :param delete_if_exists: set to True if you want an existing cluster with the same name
            to be deleted and then recreated.
        :type delete_if_exists: bool
        :param delete_upon_finish: set to True if you want the cluster to be deleted in case
            of an error.
        :type delete_upon_finish: bool
        :param reset_environment: set to True if you want Airflow database to be purged before
            reusing an existing environment. This flag is only used if reuse_if_exists is True.
        :type reset_environment: bool
        """

        if reuse_if_exists:
            log.warning("Reusing an existing Vanilla GKE environment is currently not supported.")
            reuse_if_exists = False

        super().__init__(
            namespace_prefix=AIRFLOW_NAMESPACE,
            pod_prefix=AIRFLOW_WORKER_POD_PREFIX,
            container_name=AIRFLOW_WORKER_CONTAINER_NAME,
        )

        self.elastic_dag_path = elastic_dag_path
        self.results_columns = results_columns
        self.reuse_if_exists = reuse_if_exists
        self.delete_if_exists = delete_if_exists
        self.delete_upon_finish = delete_upon_finish
        self.reset_environment = reset_environment

        jinja_variables_dict = jinja_variables_dict or {}

        environment_specification = self.load_specification(
            environment_specification_file_path,
            elastic_dag_config_file_path,
            jinja_variables_dict,
        )

        cluster_config = environment_specification["cluster_config"]
        project_id = environment_specification.get("project_id")

        self.airflow_image_tag = environment_specification.get("airflow_image_tag")
        # if docker image and tag is not specified explicitly, then a new image will be built
        # based on specified airflow version and pushed to default gcr repository
        self.docker_image = environment_specification.get("docker_image")
        # overwrites for environment variables (including Airflow config variables)
        self.env_variable_sets = environment_specification.get("env_variable_sets", {})
        # explicit overwrites for helm chart configuration;
        # will override any default sets specified in the script
        self.helm_chart_sets = environment_specification.get("helm_chart_sets", {})
        self.force_routing = environment_specification.get("force_routing", True)

        if not project_id:
            _, project_id = google.auth.default()

            if project_id is None:
                raise ValueError(
                    "Project id was not provided in specification and it "
                    "could not be retrieved from the default credentials."
                )

        self.project_id = project_id
        self.gke_cluster_disk_ids = []
        self.gke_cluster = Cluster(cluster_config)

        # pylint: enable=too-many-instance-attributes
        # pylint: enable=too-many-arguments

    @staticmethod
    def get_default_jinja_variables_values() -> Dict[str, str]:
        """
        Returns the dictionary containing default values of some configuration options that
        can be used as values for jinja variables when rendering specification file.
        """
        return {
            "airflow_image_tag": "1.10.14",
            "docker_image": "",
            "env_variable_sets": "{}",
            "helm_chart_sets": "{}",
            "force_routing": "true",
            "machine_type": "n1-standard-2",
            "disk_size_gb": "100",
            "node_count": "3",
            "max_pods_per_node": "32",
            "network_id": "default",
            "subnetwork_id": "default",
            "use_ip_aliases": "false",
            "enable_private_nodes": "false",
            "enable_private_endpoint": "false",
            "master_ipv4_cidr_block": "",  # this setting is correct for public IP env
        }

    @staticmethod
    def construct_gke_cluster_id(jinja_variables_dict: Dict[str, str]) -> str:
        """
        Creates cluster id based on the contents of the jinja variables dict.
        Note that the returned value will not necessarily contain true information
        about the cluster and it may even not be used at all - it depends on the
        specification file contents.

        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain machine_type
            and node_count keys.
        :type jinja_variables_dict: Dict[str, str]

        :return: returned cluster id
        :rtype: str
        """

        # TODO: improve this so it always uses true information from rendered file

        cluster_id = "-".join(["airflow", "{node_count}nodes", "{machine_type}"]).format(
            **jinja_variables_dict
        )

        return cluster_id.lower().replace(".", "-")[:40].strip("-")

    @staticmethod
    def validate_gke_cluster_id(cluster_id: str) -> None:
        """
        Checks provided string to determine if it fulfills the requirements of GKE cluster id.

        :param cluster_id: string to check.
        :type cluster_id: str

        :raises:
            TypeError: if cluster_id does not meet the requirements.
        """
        match = re.compile(CLUSTER_ID_REGEX).match(cluster_id)

        if match is None:
            raise ValueError(f"Cluster id '{cluster_id}' " f"does not match the regex: {CLUSTER_ID_REGEX}")

    @staticmethod
    def get_random_gke_cluster_id(cluster_id: str) -> str:
        """
        Adds a random part to the provided string to create a new GKE cluster id.
        The method assumes that cluster_id is already a valid id.
        """
        random_part = shortuuid.ShortUUID().random(length=10).lower()
        return f"{cluster_id[:(40 - len(random_part) - 1)]}-{random_part}"

    @staticmethod
    def get_airflow_env_variables_overwrites(node_count: int) -> Dict[str, Dict[str, str]]:
        """
        Returns Airflow configuration options to set on Vanilla GKE environment in order
        to make it close to the Airflow configuration of Composer environment
        with the same number of nodes.

        :param node_count: number of nodes of the GKE cluster.
        :type node_count: int

        :return: a dictionary which for Airflow configuration section names holds another
            dictionary with configuration option/value pairs
        :rtype: Dict[str, Dict[str, str]]
        """

        airflow_env_variables_overwrites = {
            "celery": {"worker_concurrency": "6"},
            "core": {
                "dag_concurrency": str(node_count * 5),
                "dags_are_paused_at_creation": "False",
                "dags_folder": DAGS_FOLDER,
                "donot_pickle": "True",
                "enable_xcom_pickling": "False",
                "max_active_runs_per_dag": str(node_count * 5),
                "parallelism": str(node_count * 10),
                "sql_alchemy_pool_recycle": "570",
            },
            # run duration in composer is passed via CLI
            "scheduler": {"run_duration": "600", "dag_dir_list_interval": "100"},
            "webserver": {"workers": "2"},
        }

        return airflow_env_variables_overwrites

    @staticmethod
    def is_private_specification(environment_specification: Dict) -> bool:
        """
        Checks if provided Vanilla GKE environment specification is set to use private GKE cluster.

        :param environment_specification: a dictionary with specification of Vanilla GKE environment
        :type environment_specification: Dict

        :return: True if given specification is configured to use private GKE cluster
            and False otherwise
        :rtype: bool
        """

        cluster_config = environment_specification["cluster_config"]

        private = cluster_config.get("private_cluster_config", {}).get(
            "enable_private_nodes", False
        ) and cluster_config.get("ip_allocation_policy", {}).get("use_ip_aliases", False)

        return private

    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments
    @classmethod
    def prepare_specifications_for_multiple_test_attempts(
        cls,
        number_of_copies: int,
        environment_specification_file_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
        randomize_environment_name: bool = True,
    ) -> Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]:
        """
        Reads provided specification file, renders it and prepares a provided number of copies of
        resulting specification.

        :param number_of_copies: number of copies of the specification that should be prepared.
        :type number_of_copies: int
        :param environment_specification_file_path: path to the file with specification
            of Vanilla GKE environment.
        :type environment_specification_file_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from environment_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]
        :param randomize_environment_name: if set to True, then a random part will be added to
            environment name of every specification copy.
        :type randomize_environment_name: bool

        :return: a tuple of two lists. Both lists contain pairs consisting of specification dicts
            and environment names:
            - first list contains specifications that can be run in parallel
            - second list contains specifications that should be run sequentially
        :rtype: Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]

        :raises: any Exception that was raised during execution of load_specification method
        """

        specifications_to_run_in_parallel = []
        specifications_to_run_in_sequence = []

        try:
            environment_specification = cls.load_specification(
                environment_specification_file_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
            )
        except Exception as exception:
            log.error(
                "Error occurred when reading environment specification file: %s\n"
                "Provided elastic dag config file: %s\n"
                "Provided jinja_variables_dict: %s",
                environment_specification_file_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
            )
            raise exception

        private = cls.is_private_specification(environment_specification)

        cluster_id = environment_specification["cluster_config"]["name"]

        for _ in range(number_of_copies):
            new_environment_specification = deepcopy(environment_specification)
            new_cluster_id = (
                cls.get_random_gke_cluster_id(cluster_id) if randomize_environment_name else cluster_id
            )
            new_environment_specification["cluster_config"]["name"] = new_cluster_id
            # TODO: need better check that would involve IP ranges and networks
            if private:
                specifications_to_run_in_sequence.append((new_environment_specification, new_cluster_id))
            else:
                specifications_to_run_in_parallel.append((new_environment_specification, new_cluster_id))

        return specifications_to_run_in_parallel, specifications_to_run_in_sequence

    # pylint: enable=too-many-locals
    # pylint: enable=too-many-arguments

    @classmethod
    def load_specification(
        cls,
        environment_specification_file_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """
        Loads the files under specified paths, validates their contents and returns Vanilla GKE
        environment specification and other environment related values.

        :param environment_specification_file_path: path to the file with specification
            of Vanilla GKE environment.
        :type environment_specification_file_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from environment_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]

        :return: a dictionary with specification of Vanilla GKE environment
        :rtype: Dict

        :raises:
            TypeError: if the specification is not a dictionary.
            KeyError: if the specification does not contain 'cluster_config' key.
        """

        jinja_variables_dict = jinja_variables_dict or {}

        jinja_variables_to_use = {
            **cls.get_default_jinja_variables_values(),
            **jinja_variables_dict,
        }

        if "cluster_id" not in jinja_variables_to_use:
            jinja_variables_to_use["cluster_id"] = cls.construct_gke_cluster_id(jinja_variables_to_use)

        environment_specification = read_templated_json_file(
            environment_specification_file_path, jinja_variables_to_use
        )

        if not isinstance(environment_specification, Dict):
            raise TypeError("Environment specification is expected to be a dictionary.")

        if "cluster_config" not in environment_specification:
            raise KeyError(
                "Specification of Vanilla GKE environment must contain 'cluster_config' key "
                "with configuration of GKE cluster."
            )

        if not environment_specification.get("airflow_image_tag") and not environment_specification.get(
            "docker_image"
        ):
            raise KeyError(
                "Specification of Vanilla GKE environment must contain either 'airflow_image_tag'"
                "key with a tag of apache/airflow Docker Hub image or 'docker_image' key with "
                "a registry path to the image already containing copies of elastic DAG file."
            )

        if not cls.is_private_specification(environment_specification):
            environment_specification["cluster_config"].pop("default_max_pods_constraint", None)
        # private clusters cannot fetch images directly from Docker Hub, as they do not have
        # outbound access to the public internet; we have to keep copies of all required images
        # on GCR and pull it from there (replace corresponding fields in values.yaml)
        else:
            raise ValueError("Using private clusters in Vanilla GKE environment is currently not supported")

        # base check of config body
        cls.validate_gke_cluster_config(environment_specification["cluster_config"])

        # overwrites for environment variables
        env_variable_sets = cls.collect_environment_variable_sets(
            environment_specification, elastic_dag_config_file_path
        )

        environment_specification["env_variable_sets"] = env_variable_sets

        return environment_specification

    @classmethod
    def validate_gke_cluster_config(cls, cluster_config: Dict) -> None:
        """
        Validates the contents of given dictionary in terms of using it to create an instance
        of GKE cluster.

        :param cluster_config: dictionary with configuration of GKE cluster.
        :type cluster_config: Dict

        :raises: TypeError, ValueError or KeyError if the configuration
            does not meet the requirements.
        """

        if "locations" not in cluster_config:
            raise KeyError(
                "GKE cluster configuration must contain 'locations' key with a list containing "
                "a single zone where cluster should be deployed."
            )

        if not isinstance(cluster_config["locations"], List):
            raise TypeError(
                "In GKE cluster configuration a list containing a single zone is expected "
                "under 'locations' key."
            )

        if len(cluster_config["locations"]) != 1:
            raise ValueError(
                "In GKE cluster configuration a single zone is expected " "in the list under 'locations' key."
            )

        if "name" not in cluster_config:
            raise KeyError("GKE cluster configuration must contain 'name' key with the id of the cluster.")

        cls.validate_gke_cluster_id(cluster_config["name"])

        # checking if node pools contains just the "default-pool" node pool
        if "node_pools" not in cluster_config:
            raise KeyError(
                f"GKE cluster configuration must contain 'node_pools' key with a list containing "
                f"a single dictionary."
            )

        if not isinstance(cluster_config["node_pools"], List):
            raise TypeError(
                "In GKE cluster configuration a list containing a single dictionary is expected "
                "under 'node_pools' key."
            )

        if len(cluster_config["node_pools"]) != 1:
            raise ValueError(
                "In GKE cluster configuration a single dictionary is expected "
                "in the list under 'node_pools' key."
            )

        if not isinstance(cluster_config["node_pools"][0], Dict):
            raise TypeError(
                "In GKE cluster configuration a single dictionary is expected "
                "in the list under 'node_pools' key."
            )

        if "initial_node_count" not in cluster_config["node_pools"][0]:
            raise KeyError(
                "In GKE cluster configuration the node pool under 'node_pools' key "
                "must contain 'initial_node_count' key with an integer value."
            )

        if not isinstance(cluster_config["node_pools"][0]["initial_node_count"], int):
            raise TypeError(
                "In GKE cluster configuration the node pool under 'node_pools' key "
                "must contain an integer value under 'initial_node_count' key."
            )

        if "name" not in cluster_config["node_pools"][0]:
            raise KeyError(
                f"In GKE cluster configuration the node pool under 'node_pools' key "
                f"must contain a 'name' key with '{DEFAULT_NODE_POOL}' as value."
            )

        if cluster_config["node_pools"][0]["name"] != "default-pool":
            raise ValueError(
                f"In GKE cluster configuration the node pool under 'node_pools' key "
                f"must be named '{DEFAULT_NODE_POOL}'"
            )

    @classmethod
    def collect_environment_variable_sets(
        cls, environment_specification: Dict, elastic_dag_conf_path: Optional[str]
    ) -> Dict[str, str]:
        """
        Reads the json file under elastic_dag_conf_path, validates its contents
        and returns the final set of environment variables to set on the environment.

        :param environment_specification: a dictionary with specification of Vanilla GKE environment
        :type environment_specification: Dict
        :param elastic_dag_conf_path: path to the file with configuration for elastic DAG.
        :type elastic_dag_conf_path: str

        :return: a dictionary with environment variables to set on the Vanilla GKE environment.
        :rtype: Dict

        :raises:
            TypeError: if elastic DAG configuration file is not a dictionary.
        """

        if elastic_dag_conf_path is not None:
            elastic_dag_conf = read_json_file(elastic_dag_conf_path)

            if not isinstance(elastic_dag_conf, Dict):
                raise TypeError("Elastic dag configuration is expected to be a dictionary.")
        else:
            elastic_dag_conf = {}

        env_variable_sets = {}
        node_count = environment_specification["cluster_config"]["node_pools"][0]["initial_node_count"]

        airflow_env_variables_overwrites = cls.get_airflow_env_variables_overwrites(node_count)

        for section in airflow_env_variables_overwrites:
            for option in airflow_env_variables_overwrites[section]:
                airflow_env_var_name = "__".join(["AIRFLOW", section.upper(), option.upper()])
                env_variable_sets[airflow_env_var_name] = airflow_env_variables_overwrites[section][option]

        # we update any default env variable sets with the ones provided explicitly in specification
        env_variable_sets.update(environment_specification.get("env_variable_sets", {}))
        # overwriting elastic dag env variables loaded from the file
        env_variable_sets.update(elastic_dag_conf)

        add_perf_start_date_env_to_conf(env_variable_sets)
        validate_elastic_dag_conf(env_variable_sets)

        return env_variable_sets

    @property
    def name(self) -> str:
        """
        Returns the name of given Vanilla GKE environment.
        """
        return self.get_gke_cluster_name()

    @property
    def states_map(self) -> Dict[State, Action]:
        """
        Returns a map specifying a method that should be executed for every applicable state
        to move the performance test forward.
        """
        return {
            State.NONE: Action(self.prepare_gke_cluster, sleep_time=None, retryable=True),
            State.WAIT_UNTIL_READY: Action(self.is_gke_cluster_ready, sleep_time=30.0, retryable=True),
            State.WAIT_UNTIL_CAN_BE_DELETED: Action(
                self.is_gke_cluster_ready, sleep_time=30.0, retryable=True
            ),
            State.DELETING_ENV: Action(self._wait_for_deletion, sleep_time=20.0, retryable=True),
            State.UPDATE_ENV_INFO: Action(self._update_environment_info, sleep_time=10.0, retryable=True),
            State.WAIT_FOR_DAG: Action(self.check_if_dags_have_loaded, sleep_time=30.0, retryable=True),
            State.UNPAUSE_DAG: Action(self.unpause_dags, sleep_time=20.0, retryable=True),
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
        if not self.check_if_gke_cluster_exists():
            return False
        cluster_state = self.get_gke_cluster_state()
        return self.state in self.terminal_states_deletable and is_state(
            cluster_state, (Cluster.Status.RUNNING, Cluster.Status.ERROR)
        )

    def prepare_gke_cluster(self) -> State:
        """
        Prepares GKE cluster the environment will be installed on - either creating
        or recreating an existing cluster depending on the flags set.
        """

        if not self.check_if_gke_cluster_exists():
            log.info("Creating environment.")

            self.cluster_manager.create_cluster(cluster=self.gke_cluster, parent=self.get_parent())
            return State.WAIT_UNTIL_READY

        cluster_state = self.get_gke_cluster_state()

        if is_state(cluster_state, Cluster.Status.STOPPING):
            log.info(
                "Cluster %s already exists but is currently being deleted. "
                "Waiting for deletion to finish before recreation.",
                self.name,
            )
            return State.DELETING_ENV

        if self.delete_if_exists:
            log.info(
                "Cluster %s already exists. Recreating it as delete-if-exists " "flag was set.",
                self.name,
            )
            return State.WAIT_UNTIL_CAN_BE_DELETED

        log.error(
            "Cluster %s already exists and delete-if-exists flag was not set. Exiting.",
            self.name,
        )
        return State.FAILED_DO_NOT_DELETE

    def _wait_for_deletion(self) -> State:
        """
        Waits until given environment ceases to exist completely.
        """
        if not self.check_if_gke_cluster_exists():
            log.info("Cluster %s was deleted.", self.name)
            return State.NONE
        return self.state

    def check_if_gke_cluster_exists(self) -> bool:
        """
        Checks if GKE cluster with given name already exists.

        :return: True if cluster exists (regardless of its state) and False otherwise.
        :rtype: bool
        """

        # TODO: use paging when listing clusters
        response = self.cluster_manager.list_clusters(parent=self.get_parent())
        cluster_id = self.get_gke_cluster_id()

        # pylint: disable=protected-access
        return any([cluster for cluster in response._pb.clusters if cluster.name == cluster_id])
        # pylint: enable=protected-access

    def is_gke_cluster_ready(self) -> State:
        """
        Waits until the process of creation/updating of GKE cluster finishes.
        """

        cluster_state = self.get_gke_cluster_state()

        not_ready_states = (Cluster.Status.PROVISIONING, Cluster.Status.RECONCILING)

        # we are still waiting, do not change the current state
        if is_state(cluster_state, not_ready_states):
            return self.state

        # the environment can be used now
        if is_state(cluster_state, Cluster.Status.RUNNING) and self.state == State.WAIT_UNTIL_READY:
            return State.UPDATE_ENV_INFO

        # the environment can be deleted now
        if (
            is_state(cluster_state, (Cluster.Status.ERROR, Cluster.Status.RUNNING))
            and self.state == State.WAIT_UNTIL_CAN_BE_DELETED
        ):
            self.delete_environment()
            return State.DELETING_ENV

        log.error(
            "An error occurred - cluster %s has reached '%s' state. Exiting.",
            self.name,
            cluster_state,
        )
        return State.FAILED

    def delete_environment(self) -> None:
        """
        Deletes the environment. This function is unsafe - it expects the proper conditions
        for environment deletion have been met: the cluster exists and is either in ERROR or
        RUNNING state.
        """
        log.info("Deleting cluster %s.", self.name)
        self.cluster_manager.delete_cluster(name=self.get_gke_cluster_name())

        # TODO: add deletion of disks assigned to the cluster
        #  - we need to make sure that remote runner provider is updated first if we are removing
        #    a pre-existing cluster with the same name (we have to basically execute
        #    _update_environment_info without install_airflow_from_helm_chart part)
        #  - disks cannot be removed until cluster is fully removed - we need to make deletion a
        #    a new state

        # for disk in self.gke_cluster_disk_ids:
        #     self.compute_client.disks().delete(
        #         project=self.project_id,
        #         zone=self.get_zone(),
        #         disk=disk["id"],
        #     ).execute()

    def _update_environment_info(self) -> State:
        """
        Updates cluster with the response from get_cluster endpoint that contains
        information available only upon cluster creation. This way some information that are not
        supposed to change for an existing environment can be retrieved without making get_cluster
        request every time.
        """

        # TODO: check if this does not contain sensitive info - maybe for private clusters?
        # pylint: disable=protected-access
        self.gke_cluster = self.get_gke_cluster()
        # pylint: enable=protected-access

        # use routing only if the public endpoint is not accessible OR the cluster is private and
        # you have forced routing
        use_routing = False
        if self.check_if_private_ip_enabled() and self.force_routing:
            use_routing = True
        elif not self.check_if_public_endpoint_enabled():
            use_routing = True

        # create an instance of kubernetes api using GKE cluster details
        self.update_remote_runner_provider(
            project_id=self.project_id,
            zone=self.get_zone(),
            cluster_id=self.get_gke_cluster_id(),
            use_routing=use_routing,
        )

        self.gke_cluster_disk_ids = self.collect_disk_ids()

        # TODO: this method is not retryable - make separate state out of it
        self.install_airflow_from_helm_chart()

        return State.WAIT_FOR_DAG

    def install_airflow_from_helm_chart(self) -> None:
        """
        Prepares docker image and installs Apache Airflow on given GKE cluster.
        """
        if self.docker_image:
            # TODO: when reusing an image we cannot be sure how many elastic dag copies
            #  are present there and if that number matches provided elastic dag configuration
            log.info("Using specified docker image: %s", self.docker_image)
            # tag "latest" seems not to work when provided to helm chart
            image_tag = self.docker_image.split(":")[-1]
        else:
            log.info("Preparing new docker image.")
            image_tag = self.publish_new_airflow_image()

        helm_install_command = self.prepare_helm_install_command(image_tag)

        # create namespace for Airflow deployment
        with self.remote_runner_provider.get_kubernetes_apis_in_isolated_context() as (
            core_api,
            _,
            proxy,
        ):
            # TODO: this needs handling reconciling cluster
            self.remote_runner_provider.create_namespace(core_api, AIRFLOW_NAMESPACE)

            execute_in_subprocess(["helm", "repo", "add", "stable", "https://charts.helm.sh/stable/"])

            execute_in_subprocess(["helm", "dep", "update", HELM_CHART_PATH])

            if proxy:
                helm_install_command = [f"https_proxy={proxy}"] + helm_install_command
            execute_in_subprocess(" ".join(helm_install_command))

    def publish_new_airflow_image(self) -> str:
        """
        Builds a new version of Airflow image with dag files uploaded to it and publishes it to
        'elastic_dag' gcr repository.
        """
        container_repository = self.get_container_repository()

        new_image_tag = str(uuid.uuid4())

        image_name = f"{container_repository}:{new_image_tag}"

        # TODO: what about specifying python version? there are differences in minor python version
        #  between helm and composer

        # TODO: with helm, worker pods do not have to be on separate nodes like in composer
        dockerfile_contents = textwrap.dedent(
            """
        FROM apache/airflow:{airflow_image_tag}
        """
        ).format(airflow_image_tag=self.airflow_image_tag)

        docker_client = docker.from_env()

        with generate_copies_of_elastic_dag(self.elastic_dag_path, self.get_env_variables()) as (
            temp_dir,
            elastic_dag_copies,
        ):

            for file_path in elastic_dag_copies:

                file_name = os.path.basename(file_path)

                dockerfile_contents += textwrap.dedent(
                    """
                COPY {file_name} {dags_folder}
                """
                ).format(file_name=file_name, dags_folder=DAGS_FOLDER)

            with open(os.path.join(temp_dir, "Dockerfile"), "w") as dockerfile:
                dockerfile.write(dockerfile_contents)

            log.info("Building image: %s", image_name)
            docker_client.images.build(path=temp_dir, rm=True, tag=image_name)

        log.info("Pushing image: %s", image_name)
        docker_client.images.push(image_name)

        return new_image_tag

    def prepare_helm_install_command(self, image_tag: str) -> List[str]:
        """
        Prepares a list with arguments for helm subprocess command that will install Airflow
        on the GKE cluster and set environment variables on it.

        :param image_tag: tag of the image to use in installation.
        :type image_tag: str

        :return: a list of arguments for helm install command.
        :rtype: List[str]
        """

        def get_helm_env_var_setter_flag(index, env_var_name, env_var_value):
            env_var_value = str(env_var_value).replace(",", "\\,")
            return [
                "--set",
                f'env[{index}].name="{env_var_name}",env[{index}].value="{env_var_value}"',
            ]

        helm_install_command = [
            "helm",
            "install",
            "airflow",
            HELM_CHART_PATH,
            "--namespace",
            "airflow",
        ]

        default_helm_chart_sets = {
            "executor": "CeleryExecutor",
            "pgbouncer.enabled": "true",
            "pgbouncer.maxClientConn": "1000",
            "workers.replicas": str(self.get_node_count()),
            # TODO: verify if disabling persistence on workers is fine
            "workers.persistence.enabled": "false",
            "images.airflow.repository": self.get_container_repository(),
            "images.airflow.tag": image_tag,
        }

        # replace the default helm chart overwrites
        # with the ones provided explicitly in specification
        self.helm_chart_sets = {**default_helm_chart_sets, **self.helm_chart_sets}

        for config_option, value in self.helm_chart_sets.items():
            helm_install_command += ["--set", f"{config_option}={value}"]

        # prepare environment variable sets
        for env_var_index, env_var in enumerate(self.env_variable_sets.items()):
            helm_install_command += get_helm_env_var_setter_flag(env_var_index, env_var[0], env_var[1])

        return helm_install_command

    def prepare_environment_columns(self) -> OrderedDict:
        """
        Prepares an OrderedDict containing environment information that will serve as columns
        for the results dataframe.

        :return: a dict with a subset of Vanilla GKE environment configuration options
            in order in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        # TODO: can drs be enabled for GKE cluster? - how to check it?
        environment_columns = OrderedDict(
            [
                ("environment_name", self.get_gke_cluster_id()),
                ("environment_type", self.environment_type),
                ("airflow_version", self.collect_airflow_version()),
                ("python_version", self.collect_python_version()),
                ("gke_version", self.get_gke_version()),
                ("environment_size", self.get_environment_size()),
                ("node_count", self.get_node_count()),
                ("machine_type", self.get_machine_type()),
                ("disk_size_gb", self.get_disk_size()),
                ("private_ip_enabled", self.check_if_private_ip_enabled()),
            ]
        )
        return environment_columns

    def get_parent(self) -> str:
        """
        Returns the string with project and location information regarding the GKE cluster.
        """
        return f"projects/{self.project_id}/locations/{self.get_zone()}"

    def get_project_id(self) -> str:
        """
        Returns the project_id of given GKE cluster.
        """
        return self.project_id

    def get_zone(self):
        """
        Returns the zone of given GKE cluster.
        """
        return self.gke_cluster.locations[0]

    def get_gke_cluster_id(self) -> str:
        """
        Returns the id of the GKE cluster.
        """
        return self.gke_cluster.name

    def get_gke_cluster_name(self) -> str:
        """
        Returns the full name of the GKE cluster.
        """
        return (
            f"projects/{self.project_id}/"
            f"locations/{self.get_zone()}/"
            f"clusters/{self.get_gke_cluster_id()}"
        )

    def get_container_repository(self) -> str:
        """
        Return the container repository where new version of Airflow image should be stored.
        """

        if self.docker_image:
            return self.docker_image.split(":")[0]

        # TODO: provide a way to specify repository to store new images in config
        return f"gcr.io/{self.project_id}/elastic_dag"

    def get_env_variables(self) -> Dict[str, str]:
        """
        Returns dictionary with environment variables set on given Vanilla GKE environment.

        :return: dict with environment variables as keys and their values as values.
        :rtype: Dict[str, str]
        """
        return self.env_variable_sets

    def get_gke_version(self) -> str:
        """
        Returns the version of GKE.
        """
        return self.gke_cluster.current_node_version

    def get_node_count(self) -> int:
        """
        Returns number of node VMs in this instance of GKE cluster.
        """
        return self.gke_cluster.current_node_count

    def get_machine_type(self) -> str:
        """
        Returns type of machine used as GKE cluster's node VMs
        in this instance of Vanilla GKE environment.
        """
        return self.gke_cluster.node_config.machine_type

    def get_disk_size(self) -> int:
        """
        Returns the disk size in GB used for node VMs
        in this instance of Vanilla GKE environment.
        """
        return self.gke_cluster.node_config.disk_size_gb

    def check_if_private_ip_enabled(self) -> bool:
        """
        Returns True if this instance of GKE cluster uses Private IP and False otherwise.
        """
        return self.gke_cluster.private_cluster_config.enable_private_nodes

    def check_if_public_endpoint_enabled(self) -> bool:
        """
        Returns True if this instance of GKE cluster has access to public master
        endpoint and False otherwise.
        """
        return not self.gke_cluster.private_cluster_config.enable_private_endpoint

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
        airflow_version = results_df["airflow_version"][0]
        python_version = results_df["python_version"][0]
        test_id = str(results_df["uuid"][0])

        return (
            test_start_day,
            environment_name,
            environment_type,
            "AIRFLOW",
            airflow_version,
            "PY",
            python_version,
            test_id,
        )
