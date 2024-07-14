from copy import deepcopy
from unittest import TestCase, mock

from kubernetes import client

from environments.kubernetes.gke.gke_remote_runner_provider import (
    GKERemoteRunnerProvider,
)

MODULE_NAME = "performance_scripts.environments.kubernetes.gke.gke_remote_runner_provider"

PROJECT_ID = "test_project_id"
ZONE = "test_zone"
CLUSTER_ID = "test_cluster_id"
DEPLOYMENT_NAME = "test_deployment_name"
COMPUTE_BASE_URL = f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/{ZONE}"
GKE_CONTEXT = f"gke_{PROJECT_ID}_{ZONE}_{CLUSTER_ID}"
NODE_POOL_NAME = f"projects/{PROJECT_ID}/locations/{ZONE}/clusters/{CLUSTER_ID}/node_pool_id/default-pool"
INSTANCE_GROUP_MANAGER = "test_instance_group_manager"
DEFAULT_NAMESPACE_PREFIX = "test_default_namespace"
NAMESPACE = "test_namespace"
NAMESPACE_PREFIX = "test_namespace_prefix"
POD_PREFIX = "test_pod_prefix"
CONTAINER = "test_container"
PORT = 2424

NAMESPACE_LISTING_RESPONSE = {
    "items": [
        {"metadata": {"name": "composer"}},
        {"metadata": {"name": "duplicate-1"}},
        {"metadata": {"name": "duplicate-2"}},
    ]
}
DEPLOYMENT_1 = {"name": DEPLOYMENT_NAME}
DEPLOYMENT_LISTING_RESPONSE = {
    "items": [
        {"metadata": DEPLOYMENT_1},
        {"metadata": {"name": "deployment-2"}},
        {"metadata": {"name": "deployment-3"}},
    ]
}
LISTING_RESPONSE_EMPTY = {"items": []}


class TestGKERemoteRunnerProvider(TestCase):
    def setUp(self):
        self.api = GKERemoteRunnerProvider(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            default_namespace_prefix=DEFAULT_NAMESPACE_PREFIX,
            use_routing=False,
        )
        self.core_api = client.CoreV1Api()
        self.apps_api = client.AppsV1Api()

    def test_gke_context(self):
        self.assertEqual(self.api.gke_context, GKE_CONTEXT)

    def test_node_pool_name(self):
        self.assertEqual(self.api.node_pool_name, NODE_POOL_NAME)

    @mock.patch(
        MODULE_NAME + ".client.api.core_v1_api.CoreV1Api.list_namespace",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(NAMESPACE_LISTING_RESPONSE)}),
    )
    def test_find_namespace_with_a_prefix(self, mock_list_namespace):
        return_value = self.api.find_namespace_with_a_prefix(self.core_api, namespace_prefix="comp")

        mock_list_namespace.assert_called_once()

        self.assertEqual(return_value, "composer")

    @mock.patch(
        MODULE_NAME + ".client.api.core_v1_api.CoreV1Api.list_namespace",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(NAMESPACE_LISTING_RESPONSE)}),
    )
    def test_find_namespace_with_a_prefix_raises_error_on_empty_results(self, mock_list_namespace):
        with self.assertRaises(ValueError):
            self.api.find_namespace_with_a_prefix(self.core_api, namespace_prefix="non-existent")

        mock_list_namespace.assert_called_once()

    @mock.patch(
        MODULE_NAME + ".client.api.core_v1_api.CoreV1Api.list_namespace",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(NAMESPACE_LISTING_RESPONSE)}),
    )
    def test_find_namespace_with_a_prefix_raises_error_on_multiple_results(self, mock_list_namespace):
        with self.assertRaises(ValueError):
            self.api.find_namespace_with_a_prefix(self.core_api, namespace_prefix="duplicate")

        mock_list_namespace.assert_called_once()

    @mock.patch(
        MODULE_NAME + ".client.api.core_v1_api.CoreV1Api.list_namespace",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(LISTING_RESPONSE_EMPTY)}),
    )
    def test_find_namespace_with_a_prefix_empty_response(self, mock_list_namespace):
        with self.assertRaises(ValueError):
            self.api.find_namespace_with_a_prefix(self.core_api, namespace_prefix="non-existent")

        mock_list_namespace.assert_called_once()

    @mock.patch(
        MODULE_NAME + ".client.api.apps_v1_api.AppsV1Api.list_namespaced_deployment",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(DEPLOYMENT_LISTING_RESPONSE)}),
    )
    def test_get_deployment(self, mock_list_namespaced_deployment):

        return_value = self.api.get_deployment(self.apps_api, DEPLOYMENT_NAME, NAMESPACE)

        mock_list_namespaced_deployment.assert_called_once_with(NAMESPACE)
        self.assertEqual(return_value, DEPLOYMENT_1)

    @mock.patch(
        MODULE_NAME + ".client.api.apps_v1_api.AppsV1Api.list_namespaced_deployment",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(DEPLOYMENT_LISTING_RESPONSE)}),
    )
    def test_get_deployment_not_found(self, mock_list_namespaced_deployment):
        return_value = self.api.get_deployment(self.apps_api, "not-exists", NAMESPACE)

        mock_list_namespaced_deployment.assert_called_once_with(NAMESPACE)
        self.assertEqual(return_value, None)

    @mock.patch(
        MODULE_NAME + ".client.api.apps_v1_api.AppsV1Api.list_namespaced_deployment",
        return_value=mock.MagicMock(**{"to_dict.return_value": deepcopy(LISTING_RESPONSE_EMPTY)}),
    )
    def test_get_deployment_empty_response(self, mock_list_namespaced_deployment):
        return_value = self.api.get_deployment(self.apps_api, "not-exists", NAMESPACE)

        mock_list_namespaced_deployment.assert_called_once_with(NAMESPACE)
        self.assertEqual(return_value, None)

    @mock.patch(MODULE_NAME + ".ClusterManagerClient")
    @mock.patch(MODULE_NAME + ".build")
    def test_node_name(self, mock_compute_client, mock_cluster_manager):
        mock_cluster_manager.return_value.get_node_pool.return_value.instance_group_urls = [
            f"{COMPUTE_BASE_URL}/instanceGroupManagers/{INSTANCE_GROUP_MANAGER}"
        ]

        # fmt: off
        mock_compute_client.return_value.instanceGroups.return_value.\
            listInstances.return_value.execute.return_value = {
                "items": [
                    {"instance": f"{COMPUTE_BASE_URL}/instances/node_1"},
                    {"instance": f"{COMPUTE_BASE_URL}/instances/node_2"},
                    {"instance": f"{COMPUTE_BASE_URL}/instances/node_3"},
                ]
            }
        # fmt: on

        # calling twice to test caching
        node_1 = self.api.node_name
        node_2 = self.api.node_name

        mock_cluster_manager.assert_called_once()
        mock_cluster_manager.return_value.get_node_pool.assert_called_once_with(name=NODE_POOL_NAME)
        mock_compute_client.assert_called_once_with("compute", "v1")
        # fmt: off
        mock_compute_client.return_value.instanceGroups.return_value.\
            listInstances.assert_called_once_with(
                project=PROJECT_ID, zone=ZONE, instanceGroup=INSTANCE_GROUP_MANAGER
            )
        # fmt: on
        self.assertEqual(node_1, node_2)
        self.assertEqual(node_1, "node_1")

    @mock.patch(MODULE_NAME + ".GKERemoteRunnerProvider.get_kubernetes_apis_in_isolated_context")
    @mock.patch(MODULE_NAME + ".GKERemoteRunnerProvider.find_namespace_with_a_prefix")
    @mock.patch(MODULE_NAME + ".RemoteRunner")
    def test_get_remote_runner(
        self,
        mock_remote_runner,
        mock_find_namespace_with_a_prefix,
        mock_get_kubernetes_apis_in_isolated_context,
    ):

        core_api_mock = mock.Mock()
        apps_api_mock = mock.Mock()
        mock_get_kubernetes_apis_in_isolated_context.return_value.__enter__.return_value = (
            core_api_mock,
            apps_api_mock,
            "",
        )

        with self.api.get_remote_runner(
            namespace_prefix=NAMESPACE_PREFIX,
            pod_prefix=POD_PREFIX,
            container=CONTAINER,
        ):
            pass

        mock_get_kubernetes_apis_in_isolated_context.assert_called_once_with()
        mock_find_namespace_with_a_prefix.assert_called_once_with(
            core_api_mock,
            NAMESPACE_PREFIX,
            [],
        )
        mock_remote_runner.assert_called_once_with(
            core_api_mock,
            mock_find_namespace_with_a_prefix.return_value,
            POD_PREFIX,
            CONTAINER,
            None,
        )

    @mock.patch(MODULE_NAME + ".GKERemoteRunnerProvider.get_kubernetes_apis_in_isolated_context")
    @mock.patch(MODULE_NAME + ".GKERemoteRunnerProvider.find_namespace_with_a_prefix")
    @mock.patch(MODULE_NAME + ".RemoteRunner")
    def test_get_remote_runner_default_namespace(
        self,
        mock_remote_runner,
        mock_find_namespace_with_a_prefix,
        mock_get_kubernetes_apis_in_isolated_context,
    ):

        core_api_mock = mock.Mock()
        apps_api_mock = mock.Mock()
        mock_get_kubernetes_apis_in_isolated_context.return_value.__enter__.return_value = (
            core_api_mock,
            apps_api_mock,
            "",
        )

        with self.api.get_remote_runner(pod_prefix=POD_PREFIX, container=CONTAINER):
            pass

        mock_get_kubernetes_apis_in_isolated_context.assert_called_once_with()
        mock_find_namespace_with_a_prefix.assert_called_once_with(
            core_api_mock,
            DEFAULT_NAMESPACE_PREFIX,
            [],
        )
        mock_remote_runner.assert_called_once_with(
            core_api_mock,
            mock_find_namespace_with_a_prefix.return_value,
            POD_PREFIX,
            CONTAINER,
            None,
        )

    @mock.patch(MODULE_NAME + ".provide_authorized_gcloud")
    @mock.patch(
        MODULE_NAME + ".GKERemoteRunnerProvider.node_name",
        new_callable=mock.PropertyMock,
    )
    @mock.patch(MODULE_NAME + ".open_routing_to_gke_node")
    @mock.patch(MODULE_NAME + ".get_cluster_credentials")
    @mock.patch(MODULE_NAME + ".config.load_kube_config")
    @mock.patch(MODULE_NAME + ".client.CoreV1Api")
    @mock.patch(MODULE_NAME + ".client.AppsV1Api")
    def test_get_kubernetes_apis_in_isolated_context(
        self,
        mock_apps_api,
        mock_core_api,
        mock_load_kube_config,
        mock_get_cluster_credentials,
        mock_open_routing_to_gke_node,
        mock_node_name,
        mock_provide_authorized_gcloud,
    ):
        with self.api.get_kubernetes_apis_in_isolated_context():
            pass

        mock_provide_authorized_gcloud.assert_called_once_with(PROJECT_ID)
        mock_node_name.assert_not_called()
        mock_open_routing_to_gke_node.assert_not_called()
        mock_get_cluster_credentials.assert_called_once_with(
            PROJECT_ID, ZONE, CLUSTER_ID, private_endpoint=False
        )
        mock_load_kube_config.assert_called_once_with(config_file=mock.ANY, context=GKE_CONTEXT)
        mock_core_api.assert_called_once_with()
        mock_apps_api.assert_called_once_with()

    @mock.patch(MODULE_NAME + ".provide_authorized_gcloud")
    @mock.patch(
        MODULE_NAME + ".GKERemoteRunnerProvider.node_name",
        new_callable=mock.PropertyMock,
    )
    @mock.patch(MODULE_NAME + ".open_routing_to_gke_node")
    @mock.patch(MODULE_NAME + ".get_cluster_credentials")
    @mock.patch(MODULE_NAME + ".config.load_kube_config")
    @mock.patch(MODULE_NAME + ".client.Configuration")
    @mock.patch(MODULE_NAME + ".client.CoreV1Api")
    @mock.patch(MODULE_NAME + ".client.AppsV1Api")
    def test_get_kubernetes_apis_in_isolated_context_with_routing(
        self,
        mock_apps_api,
        mock_core_api,
        mock_k8s_configuration,
        mock_load_kube_config,
        mock_get_cluster_credentials,
        mock_open_routing_to_gke_node,
        mock_node_name,
        mock_provide_authorized_gcloud,
    ):
        self.api.use_routing = True

        mock_open_routing_to_gke_node.return_value.__enter__.return_value = PORT

        with self.api.get_kubernetes_apis_in_isolated_context():
            pass

        mock_provide_authorized_gcloud.assert_called_once_with(PROJECT_ID)
        mock_node_name.assert_called_once()
        mock_open_routing_to_gke_node.assert_called_once_with(mock_node_name.return_value, ZONE)
        mock_get_cluster_credentials.assert_called_once_with(
            PROJECT_ID, ZONE, CLUSTER_ID, private_endpoint=True
        )
        mock_load_kube_config.assert_called_once_with(config_file=mock.ANY, context=GKE_CONTEXT)
        # pylint: disable=protected-access
        self.assertEqual(mock_k8s_configuration._default.proxy, f"http://localhost:{PORT}")
        # pylint: enable=protected-access
        mock_core_api.assert_called_once_with()
        mock_apps_api.assert_called_once_with()
