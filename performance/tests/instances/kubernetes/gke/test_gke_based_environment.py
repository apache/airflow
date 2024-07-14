import os
from unittest import TestCase, mock

import pandas as pd

from environments.base_environment import (
    FINISHED_DAG_RUN_STATES,
    State,
)
from environments.kubernetes.gke.gke_based_environment import (
    GKEBasedEnvironment,
)

MODULE_NAME = "performance_scripts.environments.kubernetes.gke.gke_based_environment"
ENVIRONMENT_SPECIFICATIONS_DIR = os.path.join(
    os.path.dirname(__file__), "environments", "environment_specifications"
)

POD_PREFIX = "test_pod_prefix"
CONTAINER_NAME = "test_container_name"
PROJECT_ID = "test_project_id"
ZONE = "test_zone"
CLUSTER_ID = "test_cluster_id"
CLUSTER_NAME = "test_cluster_name"
GKE_VERSION = "test_gke_version"
DEFAULT_NAMESPACE_PREFIX = "test_default_namespace"
DAG_PREFIX = "test_dag_prefix"
EXPECTED_DAGS_COUNT = 1
EXPECTED_DAG_RUNS_COUNT = 20
GET_DAG_RUNS_CALLS = 5
RESULTS_TABLE = {"A": [1, 2, 3, 4], "B": ["a", "b", "c", "d"]}
COLUMNS = ["A", "B"]


class TestGKEBasedEnvironment(TestCase):
    def setUp(self):
        with mock.patch(MODULE_NAME + ".ClusterManagerClient"), mock.patch(MODULE_NAME + ".build"):
            self.gke_env = GKEBasedEnvironment(
                namespace_prefix=DEFAULT_NAMESPACE_PREFIX,
                pod_prefix=POD_PREFIX,
                container_name=CONTAINER_NAME,
            )

    def test_update_remote_runner_provider(self):
        self.assertEqual(self.gke_env.remote_runner_provider, None)

        self.gke_env.update_remote_runner_provider(
            project_id=PROJECT_ID, zone=ZONE, cluster_id=CLUSTER_ID, use_routing=False
        )

        self.assertEqual(self.gke_env.remote_runner_provider.project_id, PROJECT_ID)
        self.assertEqual(self.gke_env.remote_runner_provider.zone, ZONE)
        self.assertEqual(self.gke_env.remote_runner_provider.cluster_id, CLUSTER_ID)
        self.assertEqual(
            self.gke_env.remote_runner_provider.default_namespace_prefix,
            DEFAULT_NAMESPACE_PREFIX,
        )
        self.assertEqual(self.gke_env.remote_runner_provider.use_routing, False)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_state", return_value=2)
    def test_check_cluster_readiness(self, mock_get_gke_cluster_state):

        return_value = self.gke_env.check_cluster_readiness()

        mock_get_gke_cluster_state.assert_called_once_with()
        self.assertEqual(return_value, True)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_state", return_value=3)
    def test_check_cluster_readiness__reconciling(self, mock_get_gke_cluster_state):

        return_value = self.gke_env.check_cluster_readiness()

        mock_get_gke_cluster_state.assert_called_once_with()
        self.assertEqual(return_value, False)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_state", return_value=-1)
    def test_check_cluster_readiness__unknown(self, mock_get_gke_cluster_state):

        with self.assertRaises(ValueError):
            self.gke_env.check_cluster_readiness()

        mock_get_gke_cluster_state.assert_called_once_with()

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_dags_count",
        return_value=EXPECTED_DAGS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_if_dags_have_loaded(
        self,
        mock_name,
        mock_get_dags_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dags_count.return_value": EXPECTED_DAGS_COUNT})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_if_dags_have_loaded()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_called_once_with()
        mock_get_dags_count.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_dags_count.assert_called_once_with(dag_id_prefix=DAG_PREFIX)
        mock_name.assert_called_once_with()
        self.assertEqual(new_state, State.UNPAUSE_DAG)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_dags_count",
        return_value=EXPECTED_DAGS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_if_dags_have_loaded__not_loaded(
        self,
        mock_name,
        mock_get_dags_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dags_count.return_value": 0})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_if_dags_have_loaded()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_called_once_with()
        mock_get_dags_count.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_dags_count.assert_called_once_with(dag_id_prefix=DAG_PREFIX)
        mock_name.assert_called_once_with()
        self.assertEqual(new_state, State.WAIT_FOR_DAG)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=False)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_dags_count",
        return_value=EXPECTED_DAGS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_if_dags_have_loaded__cluster_not_ready(
        self,
        mock_name,
        mock_get_dags_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dags_count.return_value": EXPECTED_DAGS_COUNT})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_if_dags_have_loaded()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_not_called()
        mock_get_dags_count.assert_not_called()
        remote_runner_provider_mock.get_remote_runner.assert_not_called()
        runner_mock.get_dags_count.assert_not_called()
        mock_name.assert_not_called()
        self.assertEqual(new_state, State.WAIT_FOR_DAG)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_unpause_dags(self, mock_name, mock_get_dag_prefix, mock_check_cluster_readiness):

        runner_mock = mock.MagicMock()

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.UNPAUSE_DAG

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.unpause_dags()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.unpause_dags.assert_called_once_with(dag_id_prefix=DAG_PREFIX)
        mock_name.assert_called_once_with()
        self.assertEqual(new_state, State.WAIT_FOR_DAG_RUN_EXEC)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=False)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_unpause_dags__cluster_not_ready(
        self, mock_name, mock_get_dag_prefix, mock_check_cluster_readiness
    ):

        runner_mock = mock.MagicMock()

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.UNPAUSE_DAG

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.unpause_dags()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_not_called()
        remote_runner_provider_mock.get_remote_runner.assert_not_called()
        runner_mock.unpause_dags.assert_not_called()
        mock_name.assert_not_called()
        self.assertEqual(new_state, State.UNPAUSE_DAG)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_expected_dag_runs_count",
        return_value=EXPECTED_DAG_RUNS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_dag_run_execution_status(
        self,
        mock_name,
        mock_get_expected_dag_runs_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dag_runs_count.return_value": 0})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG_RUN_EXEC

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_dag_run_execution_status()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_called_once_with()
        mock_get_expected_dag_runs_count.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_dag_runs_count.assert_called_once_with(
            dag_id_prefix=DAG_PREFIX, states=FINISHED_DAG_RUN_STATES
        )
        mock_name.assert_called_once_with()
        self.assertEqual(new_state, State.WAIT_FOR_DAG_RUN_EXEC)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=False)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_expected_dag_runs_count",
        return_value=EXPECTED_DAG_RUNS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_dag_run_execution_status__cluster_not_ready(
        self,
        mock_name,
        mock_get_expected_dag_runs_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dag_runs_count.return_value": EXPECTED_DAG_RUNS_COUNT})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG_RUN_EXEC

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_dag_run_execution_status()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_not_called()
        mock_get_expected_dag_runs_count.assert_not_called()
        remote_runner_provider_mock.get_remote_runner.assert_not_called()
        runner_mock.get_dag_runs_count.assert_not_called()
        mock_name.assert_not_called()
        self.assertEqual(new_state, State.WAIT_FOR_DAG_RUN_EXEC)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_expected_dag_runs_count",
        return_value=EXPECTED_DAG_RUNS_COUNT,
    )
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    def test_check_dag_run_execution_status_done(
        self,
        mock_name,
        mock_get_expected_dag_runs_count,
        mock_get_dag_prefix,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock(**{"get_dag_runs_count.return_value": EXPECTED_DAG_RUNS_COUNT})

        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )

        self.gke_env.state = State.WAIT_FOR_DAG_RUN_EXEC

        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        new_state = self.gke_env.check_dag_run_execution_status()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_get_dag_prefix.assert_called_once_with()
        mock_get_expected_dag_runs_count.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_dag_runs_count.assert_called_once_with(
            dag_id_prefix=DAG_PREFIX, states=FINISHED_DAG_RUN_STATES
        )
        mock_name.assert_called_once_with()
        self.assertEqual(new_state, State.COLLECT_RESULTS)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_environment_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_elastic_dag_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.collect_airflow_statistics")
    @mock.patch(MODULE_NAME + ".prepare_results_dataframe")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_project_id", return_value=PROJECT_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_id", return_value=CLUSTER_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_results_object_name_components")
    def test_collect_results(
        self,
        mock_get_results_object_name_components,
        mock_get_gke_cluster_id,
        mock_get_gke_project_id,
        mock_prepare_results_dataframe,
        mock_collect_airflow_statistics,
        mock_prepare_elastic_dag_columns,
        mock_prepare_environment_columns,
        mock_name,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock()
        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )
        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        mock_prepare_results_dataframe.return_value = pd.DataFrame(RESULTS_TABLE, columns=COLUMNS)

        self.gke_env.state = State.COLLECT_RESULTS

        state = self.gke_env.collect_results()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_name.assert_called_once_with()
        mock_prepare_environment_columns.assert_called_once_with()
        mock_prepare_elastic_dag_columns.assert_called_once_with()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_airflow_configuration.assert_called_once_with()
        mock_collect_airflow_statistics.assert_called_once_with(runner_mock)
        mock_get_gke_project_id.assert_called_once_with()
        mock_get_gke_cluster_id.assert_called_once_with()

        mock_prepare_results_dataframe.assert_called_once_with(
            project_id=PROJECT_ID,
            cluster_id=CLUSTER_ID,
            airflow_namespace_prefix=DEFAULT_NAMESPACE_PREFIX,
            environment_columns=mock_prepare_environment_columns.return_value,
            elastic_dag_columns=mock_prepare_elastic_dag_columns.return_value,
            airflow_configuration=runner_mock.get_airflow_configuration.return_value,
            airflow_statistics=mock_collect_airflow_statistics.return_value,
        )

        mock_get_results_object_name_components.assert_called_once_with(
            mock_prepare_results_dataframe.return_value
        )

        self.assertTrue(pd.DataFrame(RESULTS_TABLE, columns=COLUMNS).equals(self.gke_env.results[0]))
        self.assertEqual(
            self.gke_env.results[1],
            mock_get_results_object_name_components.return_value,
        )
        self.assertEqual(state, State.DONE)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=False)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_environment_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_elastic_dag_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.collect_airflow_statistics")
    @mock.patch(MODULE_NAME + ".prepare_results_dataframe")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_project_id", return_value=PROJECT_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_id", return_value=CLUSTER_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_results_object_name_components")
    def test_collect_results__cluster_not_ready(
        self,
        mock_get_results_object_name_components,
        mock_get_gke_cluster_id,
        mock_get_project_id,
        mock_prepare_results_dataframe,
        mock_collect_airflow_statistics,
        mock_prepare_elastic_dag_columns,
        mock_prepare_environment_columns,
        mock_name,
        mock_check_cluster_readiness,
    ):

        runner_mock = mock.MagicMock()
        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )
        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        mock_prepare_results_dataframe.return_value = pd.DataFrame(RESULTS_TABLE, columns=COLUMNS)

        self.gke_env.state = State.COLLECT_RESULTS

        state = self.gke_env.collect_results()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_name.assert_not_called()
        mock_prepare_environment_columns.assert_not_called()
        mock_prepare_elastic_dag_columns.assert_not_called()
        remote_runner_provider_mock.get_remote_runner.assert_not_called()
        runner_mock.get_airflow_configuration.assert_not_called()
        mock_collect_airflow_statistics.assert_not_called()
        mock_get_project_id.assert_not_called()
        mock_get_gke_cluster_id.assert_not_called()
        mock_prepare_results_dataframe.assert_not_called()
        mock_get_results_object_name_components.assert_not_called()

        self.assertEqual(state, State.COLLECT_RESULTS)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.check_cluster_readiness", return_value=True)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.name", new_callable=mock.PropertyMock)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_environment_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.prepare_elastic_dag_columns")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.collect_airflow_statistics")
    @mock.patch(MODULE_NAME + ".prepare_results_dataframe")
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_project_id", return_value=PROJECT_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_id", return_value=CLUSTER_ID)
    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_results_object_name_components")
    def test_collect_results_with_results_columns(
        self,
        mock_get_results_object_name_components,
        mock_get_gke_cluster_id,
        mock_get_gke_project_id,
        mock_prepare_results_dataframe,
        mock_collect_airflow_statistics,
        mock_prepare_elastic_dag_columns,
        mock_prepare_environment_columns,
        mock_name,
        mock_check_cluster_readiness,
    ):
        results_columns = {"B": "e", "C": True, "D": 5.0}
        expected_results_table = {
            "C": [True, True, True, True],
            "D": [5.0, 5.0, 5.0, 5.0],
            "A": [1, 2, 3, 4],
            "B": ["e", "e", "e", "e"],
        }
        expected_dataframe = pd.DataFrame(expected_results_table, columns=["C", "D", "A", "B"])

        runner_mock = mock.MagicMock()
        remote_runner_provider_mock = mock.MagicMock(
            **{"get_remote_runner.return_value.__enter__.return_value": runner_mock}
        )
        self.gke_env.remote_runner_provider = remote_runner_provider_mock

        mock_prepare_results_dataframe.return_value = pd.DataFrame(RESULTS_TABLE, columns=COLUMNS)
        self.gke_env.results_columns = results_columns

        self.gke_env.state = State.COLLECT_RESULTS

        state = self.gke_env.collect_results()

        mock_check_cluster_readiness.assert_called_once_with()
        mock_name.assert_called_once_with()
        mock_prepare_environment_columns.assert_called_once_with()
        mock_prepare_elastic_dag_columns.mock_prepare_elastic_dag_columns()
        remote_runner_provider_mock.get_remote_runner.assert_called_once_with(
            pod_prefix=POD_PREFIX, container=CONTAINER_NAME
        )
        runner_mock.get_airflow_configuration.assert_called_once_with()
        mock_collect_airflow_statistics.assert_called_once_with(runner_mock)
        mock_get_gke_project_id.assert_called_once_with()
        mock_get_gke_cluster_id.assert_called_once_with()

        mock_prepare_results_dataframe.assert_called_once_with(
            project_id=PROJECT_ID,
            cluster_id=CLUSTER_ID,
            airflow_namespace_prefix=DEFAULT_NAMESPACE_PREFIX,
            environment_columns=mock_prepare_environment_columns.return_value,
            elastic_dag_columns=mock_prepare_elastic_dag_columns.return_value,
            airflow_configuration=runner_mock.get_airflow_configuration.return_value,
            airflow_statistics=mock_collect_airflow_statistics.return_value,
        )

        mock_get_results_object_name_components.assert_called_once_with(
            mock_prepare_results_dataframe.return_value
        )

        self.assertTrue(expected_dataframe.equals(self.gke_env.results[0]))
        self.assertEqual(
            self.gke_env.results[1],
            mock_get_results_object_name_components.return_value,
        )

        self.assertEqual(state, State.DONE)

    @mock.patch(MODULE_NAME + ".GKEBasedEnvironment.get_dag_prefix", return_value=DAG_PREFIX)
    def test_collect_airflow_statistics(self, mock_get_dag_prefix):

        runner_mock = mock.MagicMock()

        results = self.gke_env.collect_airflow_statistics(runner_mock)

        mock_get_dag_prefix.assert_called_once_with()
        runner_mock.collect_dag_run_statistics.assert_called_once_with(
            dag_id_prefix=DAG_PREFIX, states=FINISHED_DAG_RUN_STATES
        )

        self.assertEqual(results, runner_mock.collect_dag_run_statistics.return_value)

    @mock.patch(
        MODULE_NAME + ".GKEBasedEnvironment.get_gke_cluster_name",
        return_value=CLUSTER_NAME,
    )
    def test_get_gke_version(self, mock_get_gke_cluster_name):

        # pylint: disable=no-member
        # pylint: disable=protected-access
        self.gke_env.cluster_manager.get_cluster.return_value._pb.current_node_version = GKE_VERSION

        return_value = self.gke_env.get_gke_version()

        mock_get_gke_cluster_name.assert_called_once_with()
        self.gke_env.cluster_manager.get_cluster.assert_called_once_with(name=CLUSTER_NAME)
        self.assertEqual(return_value, GKE_VERSION)

        # pylint: enable=no-member
        # pylint: enable=protected-access
