"""
This module contains abstract class representing communication with an environment that is based
on Google Kubernetes Engine cluster.
"""

import functools
import logging
from abc import ABC
from collections import OrderedDict
from typing import Dict, List, Optional, Sequence

from googleapiclient.discovery import build
from google.cloud.container_v1.services.cluster_manager import ClusterManagerClient
from google.cloud.container_v1.types.cluster_service import Cluster
from pandas import DataFrame

from environments.base_environment import (
    BaseEnvironment,
    FINISHED_DAG_RUN_STATES,
    State,
    is_state,
)
from environments.kubernetes.gke.collecting_results.results_dataframe import (
    prepare_results_dataframe,
)
from environments.kubernetes.remote_runner import RemoteRunner
from environments.kubernetes.gke.gke_remote_runner_provider import (
    GKERemoteRunnerProvider,
    DEFAULT_POD_PREFIX,
    DEFAULT_CONTAINER_NAME,
)
from performance_dags.elastic_dag.elastic_dag_utils import (
    calculate_number_of_dag_runs,
    get_dags_count,
    get_dag_prefix,
    prepare_elastic_dag_columns,
)


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def handle_reconciling_cluster(method):
    """
    Decorator for state methods communicating with GKE cluster via RemoteRunner, which will first
    check if the cluster is RUNNING and will immediately return the current environment state
    if it is RECONCILING.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.check_cluster_readiness():
            return self.state
        return method(self, *args, **kwargs)

    return wrapper


# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-public-methods
class GKEBasedEnvironment(BaseEnvironment, ABC):
    """
    An abstract class defining operations that are common to environments based on Google Kubernetes
    Engine cluster.
    """

    def __init__(
        self,
        namespace_prefix: str,
        pod_prefix: Optional[str] = DEFAULT_POD_PREFIX,
        container_name: Optional[str] = DEFAULT_CONTAINER_NAME,
        system_namespaces: Optional[List[str]] = None,
    ):
        # details needed to create an instance of GKERemoteRunnerProvider
        # are available only after environment is ready
        super().__init__()
        self.namespace_prefix = namespace_prefix
        self.system_namespaces = system_namespaces or []
        self.pod_prefix = pod_prefix
        self.container_name = container_name

        self.remote_runner_provider: GKERemoteRunnerProvider = None
        self.results_columns = None
        self.cluster_manager = ClusterManagerClient()
        self.compute_client = build("compute", "v1")

    # pylint: disable=too-many-arguments
    def update_remote_runner_provider(
        self, project_id: str, zone: str, cluster_id: str, use_routing: bool
    ) -> None:
        """
        Initiates an instance of GKERemoteRunnerProvider. Supposed to be called once environment is
        created and all necessary input parameters are available.

        :param project_id: Google Cloud project the GKE cluster is located in.
        :type project_id: str
        :param zone: location of GKE cluster.
        :type zone: str
        :param cluster_id: id of GKE cluster.
        :type cluster_id: str
        :param use_routing: set to True if you want to route the requests to the private master
            endpoint via one of the nodes. Obligatory if provided cluster does not have an access
            to the public master endpoint.
        :type use_routing: bool
        """

        self.remote_runner_provider = GKERemoteRunnerProvider(
            project_id=project_id,
            zone=zone,
            cluster_id=cluster_id,
            default_namespace_prefix=self.namespace_prefix,
            use_routing=use_routing,
            system_namespaces=self.system_namespaces,
        )

        # pylint: enable=too-many-arguments

    def check_cluster_readiness(self) -> bool:
        """
        Checks if GKE cluster is ready to communicate with it via kubernetes API. If cluster is
        neither in RUNNING nor RECONCILING states, an exception is raised.
        """

        cluster_state = self.get_gke_cluster_state()

        if is_state(cluster_state, Cluster.Status.RUNNING):
            log.info("GKE cluster is ready to communicate with.")
            return True

        if is_state(cluster_state, Cluster.Status.RECONCILING):
            log.info("GKE cluster is not ready to communicate with yet.")
            return False

        raise ValueError(f"GKE cluster has reached an unexpected state: {cluster_state}")

    @handle_reconciling_cluster
    def check_if_dags_have_loaded(self) -> State:
        """
        Checks if expected DAGs have already been parsed by scheduler. Moves to the next state
        if all DAGs are present in Airflow database.
        """

        dag_id_prefix = self.get_dag_prefix()

        expected_dags_count = self.get_dags_count()

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            number_of_parsed_dags = runner.get_dags_count(dag_id_prefix=dag_id_prefix)

        if number_of_parsed_dags < expected_dags_count:
            log.info(
                "Not all expected DAGs are present on environment %s. " "DAGs parsed: %d/%d. ",
                self.name,
                number_of_parsed_dags,
                expected_dags_count,
            )
            return self.state

        log.info("All expected DAGs on environment %s have been parsed.", self.name)
        return State.UNPAUSE_DAG

    @handle_reconciling_cluster
    def unpause_dags(self) -> State:
        """
        Unpauses the test dags and moves to the next state.
        """

        dag_id_prefix = self.get_dag_prefix()

        log.info("Unpausing test DAGs on environment %s.", self.name)

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            runner.unpause_dags(dag_id_prefix=dag_id_prefix)

        return State.WAIT_FOR_DAG_RUN_EXEC

    @handle_reconciling_cluster
    def check_dag_run_execution_status(self) -> State:
        """
        Checks if all test Dag Runs finished their execution.
        """

        dag_id_prefix = self.get_dag_prefix()
        expected_dag_runs_count = self.get_expected_dag_runs_count()

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            finished_dag_runs_count = runner.get_dag_runs_count(
                dag_id_prefix=dag_id_prefix, states=FINISHED_DAG_RUN_STATES
            )
        if finished_dag_runs_count < expected_dag_runs_count:
            log.info(
                "Dag runs are still executing on environment %s. " "Dag runs: %d/%d. ",
                self.name,
                finished_dag_runs_count,
                expected_dag_runs_count,
            )
            return self.state
        log.info("All expected Dag runs on environment %s have finished.", self.name)
        return State.COLLECT_RESULTS

    @handle_reconciling_cluster
    def collect_results(self) -> State:
        """
        Collects configuration information and results from different sources
        and returns a combined results dataframe and components for the result object name.
        Saves a tuple under 'results' attribute consisting of:
            - pandas Dataframe containing information about tested environment configuration and
            performance metrics
            - a sequence of strings which should be combined to create the name of the object
            where the results will be saved (for example a file, GCS blob or BQ table)
        """

        log.info("Collecting results on environment %s.", self.name)

        environment_columns = self.prepare_environment_columns()

        elastic_dag_columns = self.prepare_elastic_dag_columns()

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            airflow_configuration = runner.get_airflow_configuration()

            airflow_statistics = self.collect_airflow_statistics(runner)

        results_df = prepare_results_dataframe(
            project_id=self.get_gke_project_id(),
            cluster_id=self.get_gke_cluster_id(),
            airflow_namespace_prefix=self.namespace_prefix,
            environment_columns=environment_columns,
            elastic_dag_columns=elastic_dag_columns,
            airflow_configuration=airflow_configuration,
            airflow_statistics=airflow_statistics,
        )

        if self.results_columns is not None:
            for column_name in sorted(self.results_columns, reverse=True):
                if column_name in results_df:
                    results_df[column_name] = self.results_columns[column_name]
                else:
                    # add a new column at the beginning of the dataframe
                    results_df.insert(0, column_name, self.results_columns[column_name])

        results_object_name_components = self.get_results_object_name_components(results_df)

        self.results = (results_df, results_object_name_components)
        return State.DONE

    def collect_airflow_statistics(self, runner: RemoteRunner) -> OrderedDict:
        """
        Collects statistics of finished test Dag Runs.

        :param runner: an instance of RemoteRunner class which can be used to execute
            remote commands on GKE cluster of given environment.
        :type runner: RemoteRunner

        :return: dictionary with test statistics
            (like start and end time, average Dag Run duration).
        :rtype: OrderedDict
        """

        dag_id_prefix = self.get_dag_prefix()

        test_statistics = runner.collect_dag_run_statistics(
            dag_id_prefix=dag_id_prefix, states=FINISHED_DAG_RUN_STATES
        )

        return test_statistics

    def collect_disk_ids(self) -> List[str]:
        """
        Collects and returns ids of disks assigned to cluster's nodes.
        """

        # this should be doable even if cluster is reconciling
        list_nodes_result = self.remote_runner_provider.list_nodes()
        gke_node_urls = [node.get("instance") for node in list_nodes_result["items"]]

        disks = (
            self.compute_client.disks().list(project=self.get_project_id(), zone=self.get_zone()).execute()
        )

        gke_cluster_disk_ids = []
        for disk in disks.get("items", []):
            if any(node_url for node_url in gke_node_urls if disk.get("users") == [node_url]):
                gke_cluster_disk_ids.append(disk["id"])

        return gke_cluster_disk_ids

    def collect_python_version(self) -> str:
        """
        Collects version of python used on this environment using RemoteRunner.
        """
        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            python_version = runner.get_python_version()

        return python_version

    def collect_airflow_version(self) -> str:
        """
        Collects version of airflow used on this environment using RemoteRunner.
        """
        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            airflow_version = runner.get_airflow_version()

        return airflow_version

    def get_gke_cluster_state(self) -> str:
        """
        Returns the current state of the GKE cluster.

        :return: string with current state of the cluster.
        :rtype: str
        """
        return self.get_gke_cluster().status

    def get_gke_version(self) -> str:
        """
        Returns the version of GKE used on given cluster instance.
        """
        return self.get_gke_cluster().current_node_version

    def get_gke_cluster(self) -> Cluster:
        """
        Collects the GKE cluster given environment uses.
        """

        response = self.cluster_manager.get_cluster(name=self.get_gke_cluster_name())

        # pylint: disable=protected-access
        return response._pb
        # pylint: enable=protected-access

    def get_dag_prefix(self) -> str:
        """
        Gets a string that should be a prefix for every test DAG's dag_id.
        This allows to find Dag Runs of said test DAGs.
        """

        dag_prefix = get_dag_prefix(self.get_env_variables())

        return dag_prefix

    def get_dags_count(self) -> int:
        """
        Gets the number of test DAGs.
        """

        dags_count = get_dags_count(self.get_env_variables())

        return dags_count

    def get_expected_dag_runs_count(self) -> int:
        """
        Gets a total number of test Dag Runs that are expected to be triggered
        on given environment.
        This allows to tell when the tests have finished.
        """

        number_of_dag_runs = calculate_number_of_dag_runs(self.get_env_variables())

        return number_of_dag_runs

    def prepare_elastic_dag_columns(self) -> OrderedDict:
        """
        Prepares an OrderedDict containing elastic dag configuration that will serve as columns
        for the results dataframe.

        :return: a dict with elastic dag configuration environment variables
            in order in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        return prepare_elastic_dag_columns(self.get_env_variables())

    def get_environment_size(self) -> str:
        """
        Checks the size of the GKE cluster and returns a matching pre-defined size category.
        """
        environment_sizes = {
            "small": {
                "node_count": 3,
                "disk_size_gb": 100,
                "machine_type": "n1-standard-2",
            },
            "medium": {
                "node_count": 6,
                "disk_size_gb": 200,
                "machine_type": "n1-standard-4",
            },
            "big": {
                "node_count": 12,
                "disk_size_gb": 400,
                "machine_type": "n1-standard-8",
            },
        }

        environment_size = {
            "node_count": self.get_node_count(),
            "disk_size_gb": self.get_disk_size(),
            "machine_type": self.get_machine_type(),
        }

        for size in environment_sizes:
            if environment_size == environment_sizes[size]:
                return size

        return "custom"

    def prepare_environment_columns(self) -> OrderedDict:
        """
        This method should return an OrderedDict containing environment configuration.
        """
        raise NotImplementedError

    def get_project_id(self) -> str:
        """
        This method should return the project id the environment belongs to.
        """
        raise NotImplementedError

    def get_zone(self) -> str:
        """
        This method should return the zone the environment belongs to.
        """
        raise NotImplementedError

    def get_gke_cluster_name(self) -> str:
        """
        This method should return the full name of the GKE cluster the environment runs on.
        """
        raise NotImplementedError

    def get_gke_project_id(self) -> str:
        """
        This method should return the project id where the GKE cluster of the environment runs in.
        """
        raise NotImplementedError

    def get_gke_cluster_id(self) -> str:
        """
        This method should return the id of the GKE cluster the environment runs on.
        """
        raise NotImplementedError

    def get_results_object_name_components(self, results_df: DataFrame) -> Sequence[str]:
        """
        This method should return a sequence of components from which a results object's name can
        be formed.
        """
        raise NotImplementedError

    def get_env_variables(self) -> Dict[str, str]:
        """
        This method should return a dictionary with environment variables set on given environment.
        """
        raise NotImplementedError

    def get_node_count(self) -> int:
        """
        This method should return the number of nodes belonging to the GKE cluster.
        """
        raise NotImplementedError

    def get_disk_size(self) -> int:
        """
        This method should return a size of disks assigned to the GKE cluster's nodes.
        """
        raise NotImplementedError

    def get_machine_type(self) -> str:
        """
        This method should return the type of machine used as the GKE cluster's nodes.
        """
        raise NotImplementedError
