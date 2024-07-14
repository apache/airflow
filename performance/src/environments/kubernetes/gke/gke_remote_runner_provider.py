"""
Classes dedicated to providing RemoteRunner under GKE specific context
"""

import functools
import logging
import tempfile
from contextlib import contextmanager, ExitStack
from typing import Dict, List, Optional, Tuple
from unittest import mock

from google.cloud.container_v1.services.cluster_manager import ClusterManagerClient
from googleapiclient.discovery import build
from kubernetes import client, config

from environments.kubernetes.remote_runner import RemoteRunner
from utils.google_cloud.gcloud_utils import (
    get_cluster_credentials,
    open_routing_to_gke_node,
    provide_authorized_gcloud,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DEFAULT_NODE_POOL = "default-pool"
DEFAULT_POD_PREFIX = "airflow-worker"
DEFAULT_CONTAINER_NAME = "airflow-worker"
KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


# TODO: this class could be incorporated into GKEBasedEnvironment
class GKERemoteRunnerProvider:
    """
    Class dedicated to provide RemoteRunners under GKE context.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        project_id: str,
        zone: str,
        cluster_id: str,
        default_namespace_prefix: str,
        use_routing: bool,
        system_namespaces: Optional[List[str]] = None,
    ) -> None:
        """
        Creates an instance of GKERemoteRunnerProvider.

        :param project_id: Google Cloud project the GKE cluster is located in.
        :type project_id: str
        :param zone: location of GKE cluster.
        :type zone: str
        :param cluster_id: id of GKE cluster.
        :type cluster_id: str
        :param default_namespace_prefix: prefix of the default namespace that should be used.
        :type default_namespace_prefix: str
        :param use_routing: set to True if you want to route the requests to the private master
            endpoint via one of the nodes. Obligatory if provided cluster does not have an access
            to the public master endpoint.
        :type use_routing: bool
        :param system_namespaces: list of system namespaces.
        :type system_namespaces: Optional[List[str]]
        """

        self.project_id = project_id
        self.zone = zone
        self.cluster_id = cluster_id
        self.default_namespace_prefix = default_namespace_prefix
        self.use_routing = use_routing
        self.system_namespaces = system_namespaces or []

    # pylint: enable=too-many-arguments

    @contextmanager
    def get_remote_runner(
        self,
        namespace_prefix: Optional[str] = None,
        pod_prefix: str = DEFAULT_POD_PREFIX,
        container: str = DEFAULT_CONTAINER_NAME,
        default_request_timeout: Optional[float] = None,
    ) -> RemoteRunner:
        """
        Context manager that provides an instance of RemoteRunner in an isolated context.

        :param namespace_prefix: prefix of the namespace.
            If set to None or missing, the default namespace is used.
        :type namespace_prefix: str
        :param pod_prefix: prefix of the pod.
        :type pod_prefix: str
        :param container: name of the container.
        :type container: str
        :param default_request_timeout: the default time within which the remote commands executed
            by returned RemoteRunner instance must finish. If not provided, the default value of
            DEFAULT_EXEC_REQUEST_TIMEOUT seconds will be used.
        :type default_request_timeout: float

        :yields: an instance of RemoteRunner class operating within the prepared scope
        :type: RemoteRunner
        """

        with self.get_kubernetes_apis_in_isolated_context() as (core_api, _, _):
            namespace_prefix = namespace_prefix or self.default_namespace_prefix

            namespace = self.find_namespace_with_a_prefix(core_api, namespace_prefix, self.system_namespaces)

            yield RemoteRunner(core_api, namespace, pod_prefix, container, default_request_timeout)

    @contextmanager
    def get_kubernetes_apis_in_isolated_context(self) -> Tuple[client.CoreV1Api, client.AppsV1Api, str]:
        """
        Context manager that provides kubernetes python clients in an isolated context
        to avoid side-effects for current user. This way we can avoid changing the default
        kubernetes context (kubeconfig file). We also make sure ADC is used for authorization
        when calling gcloud instead of the account that is currently set up in gcloud.

        :yields: a tuple of CoreV1Api and AppsV1Api instances and the proxy
        :type: Tuple[client.CoreV1Api, client.AppsV1Api, str]
        """

        with ExitStack() as exit_stack:
            # Authorize gcloud tool
            exit_stack.enter_context(provide_authorized_gcloud(self.project_id))
            # Prepare a new temporary kubeconfig
            kubectl_temp_conf_file = exit_stack.enter_context(tempfile.NamedTemporaryFile())
            exit_stack.enter_context(
                mock.patch.dict("os.environ", {KUBE_CONFIG_ENV_VAR: kubectl_temp_conf_file.name})
            )
            # Open IAP proxy for private connection
            if self.use_routing:
                http_port = exit_stack.enter_context(open_routing_to_gke_node(self.node_name, self.zone))
            else:
                http_port = None

            log.info("Collecting cluster credentials.")

            get_cluster_credentials(
                self.project_id,
                self.zone,
                self.cluster_id,
                private_endpoint=self.use_routing,
            )

            log.info("Loading kubernetes configuration.")

            config.load_kube_config(config_file=kubectl_temp_conf_file.name, context=self.gke_context)

            proxy = ""

            if http_port is not None:
                log.info(
                    "Setting proxy for communication with private cluster on port: %s",
                    http_port,
                )
                proxy = f"http://localhost:{http_port}"
                # pylint: disable=protected-access
                client.Configuration._default.proxy = proxy
                # pylint: enable=protected-access

            core_api = client.CoreV1Api()
            apps_api = client.AppsV1Api()

            yield core_api, apps_api, proxy

    @property
    @functools.lru_cache()
    def node_name(self) -> str:
        """
        Finds the first node of given cluster without using kubernetes API. Designed to retrieve
        nodes of private clusters (without using kubernetes api). Executed once, after which the
        result is cached.

        :return: name of the first node found.
        :rtype: str
        """

        log.info("Collecting node of a private cluster.")

        nodes_list = self.list_nodes()

        return nodes_list["items"][0]["instance"].split("instances/")[-1]

    def list_nodes(self):
        """
        Lists nodes of given cluster.
        """

        gke_client = ClusterManagerClient()

        log.info("Collecting node pool: %s.", self.node_pool_name)

        node_pool_details = gke_client.get_node_pool(name=self.node_pool_name)

        instance_group_url = node_pool_details.instance_group_urls[0]

        instance_group = instance_group_url.split("instanceGroupManagers/")[-1]

        compute_client = build("compute", "v1")

        log.info("Collecting nodes belonging to instance group: %s.", instance_group)

        list_instances_result = (
            compute_client.instanceGroups()
            .listInstances(project=self.project_id, zone=self.zone, instanceGroup=instance_group)
            .execute()
        )

        return list_instances_result

    @property
    def node_pool_name(self) -> str:
        """
        Returns the name of the default node pool for given cluster.
        """

        return (
            f"projects/{self.project_id}/"
            f"locations/{self.zone}/"
            f"clusters/{self.cluster_id}/"
            f"node_pool_id/{DEFAULT_NODE_POOL}"
        )

    @property
    def gke_context(self) -> str:
        """
        Returns the kubernetes context for given GKE cluster.
        """

        return f"gke_{self.project_id}_{self.zone}_{self.cluster_id}"

    # TODO: this should be moved to the kubernetes directory as it does not depend on gke
    @staticmethod
    def find_namespace_with_a_prefix(
        core_api: client.api.core_v1_api.CoreV1Api,
        namespace_prefix: str,
        ignore_namespaces: Optional[List[str]] = None,
    ) -> str:
        """
        Finds the namespace which starts with namespace_prefix.

        :param core_api: an instance of kubernetes python core v1 api which can be used
            to find the namespace.
        :type core_api: client.api.core_v1_api.CoreV1Api
        :param namespace_prefix: prefix of the namespace.
        :type namespace_prefix: str
        :param ignore_namespaces: namespaces to ignore.
        :type ignore_namespaces: Optional[List[str]]

        :return: found namespace.
        :rtype: str

        :raises: ValueError: if no namespace was found or multiple namespaces were found.
        """
        if ignore_namespaces is None:
            ignore_namespaces = []

        log.info("Collecting namespace prefixed with '%s'.", namespace_prefix)

        response = core_api.list_namespace()

        metadata_list = [item["metadata"] for item in response.to_dict()["items"]]

        namespace = [
            metadata["name"]
            for metadata in metadata_list
            if metadata["name"]
            and metadata["name"].startswith(namespace_prefix)
            and metadata["name"] not in ignore_namespaces
        ]

        if not namespace or len(namespace) > 1:
            raise ValueError(f"Could not find a single namespace starting with '{namespace_prefix}'.")
        return namespace[0]

    # TODO: this should be moved to the kubernetes directory as it does not depend on gke
    @staticmethod
    def create_namespace(service: client.api.core_v1_api.CoreV1Api, namespace_name: str) -> None:
        """
        Creates the namespace with specified name.

        :param service: an instance of kubernetes python client which should be used
            to create the namespace.
        :type service: client.api.core_v1_api.CoreV1Api
        :param namespace_name: name of the namespace.
        :type namespace_name: str
        """

        log.info("Creating namespace '%s'.", namespace_name)

        namespace_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace_name))

        service.create_namespace(namespace_body)

    # TODO: this could be moved to the kubernetes directory as it does not depend on gke
    @staticmethod
    def get_deployment(
        apps_api: client.api.apps_v1_api.AppsV1Api, deployment_name: str, namespace: str
    ) -> Optional[Dict]:
        """
        Collects the deployment which provided name belonging to specified namespace.

        :param apps_api: an instance of kubernetes python apps v1 api which can be used
            to find the deployment.
        :type apps_api: client.api.apps_v1_api.AppsV1Api
        :param deployment_name: name of the deployment.
        :type deployment_name: str
        :param namespace: namespace of the deployment.
        :type namespace: str

        :return: found deployment or None, if no deployment was found.
        :rtype: Dict
        """

        log.info(
            "Collecting deployment '%s' from namespace '%s'.",
            deployment_name,
            namespace,
        )

        response = apps_api.list_namespaced_deployment(namespace)

        metadata_list = [item["metadata"] for item in response.to_dict()["items"]]

        deployments = [metadata for metadata in metadata_list if metadata["name"] == deployment_name]

        if not deployments:
            return None
        return deployments[0]
