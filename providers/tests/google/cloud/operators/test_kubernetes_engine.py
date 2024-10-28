#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import os
from unittest import mock
from unittest.mock import mock_open

import pytest
from google.cloud.container_v1.types import Cluster, NodePool
from kubernetes.client.models import V1Deployment, V1DeploymentStatus
from kubernetes.utils.create_from_yaml import FailToCreateError

from airflow.exceptions import (
    AirflowException,
    AirflowProviderDeprecationWarning,
    TaskDeferred,
)
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import (
    KubernetesDeleteJobOperator,
    KubernetesJobOperator,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKECreateCustomResourceOperator,
    GKEDeleteClusterOperator,
    GKEDeleteCustomResourceOperator,
    GKEDeleteJobOperator,
    GKEDescribeJobOperator,
    GKEResumeJobOperator,
    GKEStartJobOperator,
    GKEStartKueueInsideClusterOperator,
    GKEStartKueueJobOperator,
    GKEStartPodOperator,
    GKESuspendJobOperator,
)
from airflow.providers.google.cloud.triggers.kubernetes_engine import GKEStartPodTrigger

TEST_GCP_PROJECT_ID = "test-id"
PROJECT_LOCATION = "test-location"
PROJECT_TASK_ID = "test-task-id"
CLUSTER_NAME = "test-cluster-name"
QUEUE_NAME = "test-queue-name"

PROJECT_BODY = {"name": "test-name"}
PROJECT_BODY_CREATE_DICT = {"name": "test-name", "initial_node_count": 1}
PROJECT_BODY_CREATE_DICT_NODE_POOLS = {
    "name": "test-name",
    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
}

PROJECT_BODY_CREATE_CLUSTER = Cluster(name="test-name", initial_node_count=1)
PROJECT_BODY_CREATE_CLUSTER_NODE_POOLS = Cluster(
    name="test-name", node_pools=[NodePool(name="a_node_pool", initial_node_count=1)]
)

TASK_NAME = "test-task-name"
JOB_NAME = "test-job"
POD_NAME = "test-pod"
NAMESPACE = ("default",)
IMAGE = "bash"
JOB_POLL_INTERVAL = 20.0

GCLOUD_COMMAND = "gcloud container clusters get-credentials {} --zone {} --project {}"
KUBE_ENV_VAR = "KUBECONFIG"
FILE_NAME = "/tmp/mock_name"
KUB_OP_PATH = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.{}"
GKE_HOOK_MODULE_PATH = "airflow.providers.google.cloud.operators.kubernetes_engine"
GKE_HOOK_PATH = f"{GKE_HOOK_MODULE_PATH}.GKEHook"
GKE_KUBERNETES_HOOK = f"{GKE_HOOK_MODULE_PATH}.GKEKubernetesHook"
GKE_K8S_HOOK_PATH = f"{GKE_HOOK_MODULE_PATH}.GKEKubernetesHook"
KUB_OPERATOR_EXEC = (
    "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.execute"
)
KUB_JOB_OPERATOR_EXEC = (
    "airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator.execute"
)
KUB_CREATE_RES_OPERATOR_EXEC = "airflow.providers.cncf.kubernetes.operators.resource.KubernetesCreateResourceOperator.execute"
KUB_DELETE_RES_OPERATOR_EXEC = "airflow.providers.cncf.kubernetes.operators.resource.KubernetesDeleteResourceOperator.execute"
DEL_KUB_JOB_OPERATOR_EXEC = (
    "airflow.providers.cncf.kubernetes.operators.job.KubernetesDeleteJobOperator.execute"
)
TEMP_FILE = "tempfile.NamedTemporaryFile"
GKE_OP_PATH = (
    "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator"
)
GKE_CREATE_CLUSTER_PATH = (
    "airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator"
)
GKE_JOB_OP_PATH = (
    "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartJobOperator"
)
GKE_CLUSTER_AUTH_DETAILS_PATH = (
    "airflow.providers.google.cloud.operators.kubernetes_engine.GKEClusterAuthDetails"
)
CLUSTER_URL = "https://test-host"
CLUSTER_PRIVATE_URL = "https://test-private-host"
SSL_CA_CERT = "TEST_SSL_CA_CERT_CONTENT"
KUEUE_VERSION = "v0.5.1"
IMPERSONATION_CHAIN = "sa-@google.com"
USE_INTERNAL_API = False
READY_DEPLOYMENT = V1Deployment(
    status=V1DeploymentStatus(
        observed_generation=1,
        ready_replicas=1,
        replicas=1,
        unavailable_replicas=None,
        updated_replicas=1,
    )
)
VALID_RESOURCE_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test_pvc
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 5Gi
"""
KUEUE_YAML_URL = "http://test-url/config.yaml"


class TestGoogleCloudPlatformContainerOperator:
    @pytest.mark.parametrize(
        "body",
        [
            PROJECT_BODY_CREATE_DICT,
            PROJECT_BODY_CREATE_DICT_NODE_POOLS,
            PROJECT_BODY_CREATE_CLUSTER,
            PROJECT_BODY_CREATE_CLUSTER_NODE_POOLS,
        ],
    )
    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute(self, mock_hook, body):
        print("type: ", type(body))
        if body == PROJECT_BODY_CREATE_DICT or body == PROJECT_BODY_CREATE_CLUSTER:
            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="The body field 'initial_node_count' is deprecated. Use 'node_pool.initial_node_count' instead.",
            ):
                operator = GKECreateClusterOperator(
                    project_id=TEST_GCP_PROJECT_ID,
                    location=PROJECT_LOCATION,
                    body=body,
                    task_id=PROJECT_TASK_ID,
                )
        else:
            operator = GKECreateClusterOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                body=body,
                task_id=PROJECT_TASK_ID,
            )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.create_cluster.assert_called_once_with(
            cluster=body,
            project_id=TEST_GCP_PROJECT_ID,
            wait_to_complete=True,
        )

    @pytest.mark.parametrize(
        "body",
        [
            None,
            {"missing_name": "test-name", "initial_node_count": 1},
            {
                "name": "test-name",
                "initial_node_count": 1,
                "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            {
                "missing_name": "test-name",
                "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            {
                "name": "test-name",
                "missing_initial_node_count": 1,
                "missing_node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            type(
                "Cluster",
                (object,),
                {"missing_name": "test-name", "initial_node_count": 1},
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "missing_name": "test-name",
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "missing_initial_node_count": 1,
                    "missing_node_pools": [
                        {"name": "a_node_pool", "initial_node_count": 1}
                    ],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "initial_node_count": 1,
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
        ],
    )
    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_body(self, mock_hook, body):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                body=body,
                task_id=PROJECT_TASK_ID,
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_project_id(self, mock_hook):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(
                location=PROJECT_LOCATION, body=PROJECT_BODY, task_id=PROJECT_TASK_ID
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_location(self, mock_hook):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, body=PROJECT_BODY, task_id=PROJECT_TASK_ID
            )

    @mock.patch("airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook")
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator.defer"
    )
    def test_create_execute_call_defer_method(self, mock_defer_method, mock_hook):
        operator = GKECreateClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            body=PROJECT_BODY_CREATE_DICT_NODE_POOLS,
            task_id=PROJECT_TASK_ID,
            deferrable=True,
        )

        operator.execute(mock.MagicMock())

        mock_defer_method.assert_called_once()

    @mock.patch("airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook")
    def test_delete_execute(self, mock_hook):
        operator = GKEDeleteClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            name=CLUSTER_NAME,
            location=PROJECT_LOCATION,
            task_id=PROJECT_TASK_ID,
        )

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            name=CLUSTER_NAME,
            project_id=TEST_GCP_PROJECT_ID,
            wait_to_complete=True,
        )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_project_id(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(
                location=PROJECT_LOCATION, name=CLUSTER_NAME, task_id=PROJECT_TASK_ID
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_cluster_name(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                task_id=PROJECT_TASK_ID,
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_location(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, name=CLUSTER_NAME, task_id=PROJECT_TASK_ID
            )

    @mock.patch("airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook")
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator.defer"
    )
    def test_delete_execute_call_defer_method(self, mock_defer_method, mock_hook):
        operator = GKEDeleteClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            name=CLUSTER_NAME,
            location=PROJECT_LOCATION,
            task_id=PROJECT_TASK_ID,
            deferrable=True,
        )

        operator.execute(None)

        mock_defer_method.assert_called_once()


class TestGKEPodOperator:
    def setup_method(self):
        self.gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            on_finish_action=OnFinishAction.KEEP_POD,
        )
        self.gke_op.pod = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_template_fields(self):
        assert set(KubernetesPodOperator.template_fields).issubset(
            GKEStartPodOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute(self, fetch_cluster_info_mock, file_mock, exec_mock):
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEStartPodOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                image=IMAGE,
                config_file="/path/to/alternative/kubeconfig",
                on_finish_action=OnFinishAction.KEEP_POD,
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute_with_impersonation_service_account(
        self, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute_with_impersonation_service_chain_one_element(
        self, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.parametrize("use_internal_ip", [True, False])
    @mock.patch(f"{GKE_HOOK_PATH}.get_cluster")
    def test_cluster_info(self, get_cluster_mock, use_internal_ip):
        get_cluster_mock.return_value = mock.MagicMock(
            **{
                "endpoint": "test-host",
                "private_cluster_config.private_endpoint": "test-private-host",
                "master_auth.cluster_ca_certificate": SSL_CA_CERT,
            }
        )
        gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            use_internal_ip=use_internal_ip,
            on_finish_action=OnFinishAction.KEEP_POD,
        )
        cluster_url, ssl_ca_cert = gke_op.fetch_cluster_info()

        assert cluster_url == CLUSTER_PRIVATE_URL if use_internal_ip else CLUSTER_URL
        assert ssl_ca_cert == SSL_CA_CERT

    @pytest.mark.db_test
    def test_default_gcp_conn_id(self):
        gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            on_finish_action=OnFinishAction.KEEP_POD,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    def test_gcp_conn_id(self, get_con_mock):
        gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            gcp_conn_id="test_conn",
            on_finish_action=OnFinishAction.KEEP_POD,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"

    @pytest.mark.parametrize(
        "compatible_kpo, kwargs, expected_attributes",
        [
            (
                True,
                {"on_finish_action": "delete_succeeded_pod"},
                {"on_finish_action": OnFinishAction.DELETE_SUCCEEDED_POD},
            ),
            (
                # test that priority for deprecated param
                True,
                {"on_finish_action": "keep_pod", "is_delete_operator_pod": True},
                {
                    "on_finish_action": OnFinishAction.DELETE_POD,
                    "is_delete_operator_pod": True,
                },
            ),
            (
                # test default
                True,
                {},
                {
                    "on_finish_action": OnFinishAction.KEEP_POD,
                    "is_delete_operator_pod": False,
                },
            ),
            (
                False,
                {"is_delete_operator_pod": True},
                {"is_delete_operator_pod": True},
            ),
            (
                False,
                {"is_delete_operator_pod": False},
                {"is_delete_operator_pod": False},
            ),
            (
                # test default
                False,
                {},
                {"is_delete_operator_pod": False},
            ),
        ],
    )
    def test_on_finish_action_handler(
        self,
        compatible_kpo,
        kwargs,
        expected_attributes,
    ):
        kpo_init_args_mock = mock.MagicMock(
            **{"parameters": ["on_finish_action"] if compatible_kpo else []}
        )

        with mock.patch("inspect.signature", return_value=kpo_init_args_mock):
            if "is_delete_operator_pod" in kwargs:
                with pytest.warns(
                    AirflowProviderDeprecationWarning,
                    match="`is_delete_operator_pod` parameter is deprecated, please use `on_finish_action`",
                ):
                    op = GKEStartPodOperator(
                        project_id=TEST_GCP_PROJECT_ID,
                        location=PROJECT_LOCATION,
                        cluster_name=CLUSTER_NAME,
                        task_id=PROJECT_TASK_ID,
                        name=TASK_NAME,
                        namespace=NAMESPACE,
                        image=IMAGE,
                        **kwargs,
                    )
            elif "on_finish_action" not in kwargs:
                with pytest.warns(
                    AirflowProviderDeprecationWarning,
                    match="You have not set parameter `on_finish_action` in class GKEStartPodOperator. Currently the default for this parameter is `keep_pod` but in a future release the default will be changed to `delete_pod`. To ensure pods are not deleted in the future you will need to set `on_finish_action=keep_pod` explicitly.",
                ):
                    op = GKEStartPodOperator(
                        project_id=TEST_GCP_PROJECT_ID,
                        location=PROJECT_LOCATION,
                        cluster_name=CLUSTER_NAME,
                        task_id=PROJECT_TASK_ID,
                        name=TASK_NAME,
                        namespace=NAMESPACE,
                        image=IMAGE,
                        **kwargs,
                    )
            else:
                op = GKEStartPodOperator(
                    project_id=TEST_GCP_PROJECT_ID,
                    location=PROJECT_LOCATION,
                    cluster_name=CLUSTER_NAME,
                    task_id=PROJECT_TASK_ID,
                    name=TASK_NAME,
                    namespace=NAMESPACE,
                    image=IMAGE,
                    **kwargs,
                )
            for expected_attr in expected_attributes:
                assert (
                    op.__getattribute__(expected_attr)
                    == expected_attributes[expected_attr]
                )


class TestGKEStartKueueInsideClusterOperator:
    @pytest.fixture(autouse=True)
    def setup_test(self):
        self.gke_op = GKEStartKueueInsideClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            kueue_version=KUEUE_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_internal_ip=USE_INTERNAL_API,
        )
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT

    @pytest.mark.flaky(reruns=5)
    @pytest.mark.db_test
    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(f"{GKE_KUBERNETES_HOOK}.check_kueue_deployment_running")
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute(
        self,
        mock_pod_hook,
        mock_deployment,
        mock_hook,
        fetch_cluster_info_mock,
        file_mock,
    ):
        mock_pod_hook.return_value.apply_from_yaml_file.side_effect = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = PROJECT_BODY_CREATE_CLUSTER
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.flaky(reruns=5)
    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_KUBERNETES_HOOK)
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute_autoscaled_cluster(
        self,
        mock_pod_hook,
        mock_hook,
        mock_depl_hook,
        fetch_cluster_info_mock,
        file_mock,
        caplog,
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = mock.MagicMock()
        mock_pod_hook.return_value.apply_from_yaml_file.side_effect = mock.MagicMock()
        mock_hook.return_value.check_cluster_autoscaling_ability.return_value = True
        mock_depl_hook.return_value.get_deployment_status.return_value = READY_DEPLOYMENT
        self.gke_op.execute(context=mock.MagicMock())

        assert "Kueue installed successfully!" in caplog.text

    @pytest.mark.flaky(reruns=5)
    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute_autoscaled_cluster_check_error(
        self, mock_pod_hook, mock_hook, fetch_cluster_info_mock, file_mock, caplog
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = mock.MagicMock()
        mock_hook.return_value.check_cluster_autoscaling_ability.return_value = True
        mock_pod_hook.return_value.apply_from_yaml_file.side_effect = FailToCreateError(
            "error"
        )
        self.gke_op.execute(context=mock.MagicMock())

        assert "Kueue is already enabled for the cluster" in caplog.text

    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute_non_autoscaled_cluster_check_error(
        self, mock_pod_hook, mock_hook, fetch_cluster_info_mock, file_mock, caplog
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = mock.MagicMock()
        mock_hook.return_value.check_cluster_autoscaling_ability.return_value = False
        self.gke_op.execute(context=mock.MagicMock())

        assert (
            "Cluster doesn't have ability to autoscale, will not install Kueue inside. Aborting"
            in caplog.text
        )
        mock_pod_hook.assert_not_called()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = PROJECT_BODY_CREATE_CLUSTER
        mock_hook.return_value.check_cluster_autoscaling_ability.return_value = False
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        mock_hook.return_value.get_cluster.return_value = PROJECT_BODY_CREATE_CLUSTER
        mock_hook.return_value.check_cluster_autoscaling_ability.return_value = False
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    def test_default_gcp_conn_id(self):
        gke_op = GKEStartKueueInsideClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            kueue_version=KUEUE_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_internal_ip=USE_INTERNAL_API,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.cluster_hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=Connection(
            extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
        ),
    )
    def test_gcp_conn_id(self, mock_get_credentials):
        gke_op = GKEStartKueueInsideClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            kueue_version=KUEUE_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_internal_ip=USE_INTERNAL_API,
            gcp_conn_id="test_conn",
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.cluster_hook

        assert hook.gcp_conn_id == "test_conn"

    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.requests")
    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.yaml")
    def test_get_yaml_content_from_file(self, mock_yaml, mock_requests):
        yaml_content_expected = [mock.MagicMock(), mock.MagicMock()]
        mock_yaml.safe_load_all.return_value = yaml_content_expected
        response_text_expected = "response test expected"
        mock_requests.get.return_value = mock.MagicMock(
            status_code=200, text=response_text_expected
        )

        yaml_content_actual = (
            GKEStartKueueInsideClusterOperator._get_yaml_content_from_file(KUEUE_YAML_URL)
        )

        assert yaml_content_actual == yaml_content_expected
        mock_requests.get.assert_called_once_with(KUEUE_YAML_URL, allow_redirects=True)
        mock_yaml.safe_load_all.assert_called_once_with(response_text_expected)

    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.requests")
    def test_get_yaml_content_from_file_exception(self, mock_requests):
        mock_requests.get.return_value = mock.MagicMock(status_code=400)

        with pytest.raises(AirflowException):
            GKEStartKueueInsideClusterOperator._get_yaml_content_from_file(KUEUE_YAML_URL)


class TestGKEPodOperatorAsync:
    def setup_method(self):
        self.gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            deferrable=True,
            on_finish_action="delete_pod",
        )
        self.gke_op.pod = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_OP_PATH.format("build_pod_request_obj"))
    @mock.patch(KUB_OP_PATH.format("get_or_create_pod"))
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_async_create_pod_should_execute_successfully(
        self, fetch_cluster_info_mock, get_con_mock, mocked_pod, mocked_pod_obj, mocker
    ):
        """
        Asserts that a task is deferred and the GKEStartPodTrigger will be fired
        when the GKEStartPodOperator is executed in deferrable mode when deferrable=True.
        """
        mock_file = mock_open(read_data='{"a": "b"}')
        mocker.patch("builtins.open", mock_file)

        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT
        with pytest.raises(TaskDeferred) as exc:
            self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()
        assert isinstance(exc.value.trigger, GKEStartPodTrigger)

    @pytest.mark.parametrize("status", ["error", "failed", "timeout"])
    @mock.patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod"
    )
    @mock.patch(KUB_OP_PATH.format("_clean"))
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.hook"
    )
    @mock.patch(KUB_OP_PATH.format("_write_logs"))
    def test_execute_complete_failure(
        self, mock_write_logs, mock_gke_hook, mock_clean, mock_get_pod, status
    ):
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT
        with pytest.raises(AirflowException):
            self.gke_op.execute_complete(
                context=mock.MagicMock(),
                event={
                    "name": "test",
                    "status": status,
                    "namespace": "default",
                    "message": "",
                },
                cluster_url=self.gke_op._cluster_url,
                ssl_ca_cert=self.gke_op._ssl_ca_cert,
            )
        mock_write_logs.assert_called_once()

    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.hook"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod"
    )
    @mock.patch(KUB_OP_PATH.format("_clean"))
    @mock.patch(KUB_OP_PATH.format("_write_logs"))
    def test_execute_complete_success(
        self, mock_write_logs, mock_clean, mock_get_pod, mock_gke_hook
    ):
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT
        self.gke_op.execute_complete(
            context=mock.MagicMock(),
            event={"name": "test", "status": "success", "namespace": "default"},
            cluster_url=self.gke_op._cluster_url,
            ssl_ca_cert=self.gke_op._ssl_ca_cert,
        )
        mock_write_logs.assert_called_once()

    @mock.patch(KUB_OP_PATH.format("pod_manager"))
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.invoke_defer_method"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod"
    )
    @mock.patch(KUB_OP_PATH.format("_clean"))
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.hook"
    )
    def test_execute_complete_running(
        self,
        mock_gke_hook,
        mock_clean,
        mock_get_pod,
        mock_invoke_defer_method,
        mock_pod_manager,
    ):
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT
        self.gke_op.execute_complete(
            context=mock.MagicMock(),
            event={"name": "test", "status": "running", "namespace": "default"},
            cluster_url=self.gke_op._cluster_url,
            ssl_ca_cert=self.gke_op._ssl_ca_cert,
        )
        mock_pod_manager.fetch_container_logs.assert_called_once()
        mock_invoke_defer_method.assert_called_once()


class TestGKEStartJobOperator:
    def setup_method(self):
        self.gke_op = GKEStartJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
        )
        self.gke_op.job = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_template_fields(self):
        assert set(KubernetesJobOperator.template_fields).issubset(
            GKEStartJobOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute(self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.ProvidersManager")
    def test_execute_in_deferrable_mode(
        self, mock_providers_manager, mock_hook, fetch_cluster_info_mock, exec_mock
    ):
        kubernetes_package_name = "apache-airflow-providers-cncf-kubernetes"
        mock_providers_manager.return_value.providers = {
            kubernetes_package_name: mock.MagicMock(
                data={
                    "package-name": kubernetes_package_name,
                },
                version="8.0.2",
            )
        }
        self.gke_op.deferrable = True

        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.ProvidersManager")
    def test_execute_in_deferrable_mode_exception(self, mock_providers_manager):
        kubernetes_package_name = "apache-airflow-providers-cncf-kubernetes"
        mock_providers_manager.return_value.providers = {
            kubernetes_package_name: mock.MagicMock(
                data={
                    "package-name": kubernetes_package_name,
                },
                version="8.0.1",
            )
        }
        self.gke_op.deferrable = True
        with pytest.raises(AirflowException):
            self.gke_op.execute({})

    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.GKEJobTrigger")
    def test_execute_deferrable(self, mock_trigger):
        mock_trigger_instance = mock_trigger.return_value

        op = GKEStartJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            job_poll_interval=JOB_POLL_INTERVAL,
        )
        op._ssl_ca_cert = SSL_CA_CERT
        op._cluster_url = CLUSTER_URL

        with mock.patch.object(op, "job") as mock_job:
            mock_metadata = mock_job.metadata
            mock_metadata.name = TASK_NAME
            mock_metadata.namespace = NAMESPACE

            mock_pod = mock.MagicMock()
            mock_pod.metadata.name = POD_NAME
            mock_pod.metadata.namespace = NAMESPACE
            op.pod = mock_pod

            with mock.patch.object(op, "defer") as mock_defer:
                op.execute_deferrable()

        mock_trigger.assert_called_once_with(
            cluster_url=CLUSTER_URL,
            ssl_ca_cert=SSL_CA_CERT,
            job_name=TASK_NAME,
            job_namespace=NAMESPACE,
            pod_name=POD_NAME,
            pod_namespace=NAMESPACE,
            base_container_name=op.BASE_CONTAINER_NAME,
            gcp_conn_id="google_cloud_default",
            poll_interval=JOB_POLL_INTERVAL,
            impersonation_chain=None,
            get_logs=True,
            do_xcom_push=False,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger_instance,
            method_name="execute_complete",
            kwargs={"cluster_url": CLUSTER_URL, "ssl_ca_cert": SSL_CA_CERT},
        )

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEStartJobOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                image=IMAGE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    def test_default_gcp_conn_id(self):
        gke_op = GKEStartJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    def test_gcp_conn_id(self, get_con_mock):
        gke_op = GKEStartJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            gcp_conn_id="test_conn",
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"


class TestGKEDescribeJobOperator:
    def setup_method(self):
        self.gke_op = GKEDescribeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            job_name=JOB_NAME,
            namespace=NAMESPACE,
        )
        self.gke_op.job = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute(self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute_with_impersonation_service_account(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_KUBERNETES_HOOK)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    def test_default_gcp_conn_id(self, fetch_cluster_info_mock):
        gke_op = GKEDescribeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            job_name=TASK_NAME,
            namespace=NAMESPACE,
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_gcp_conn_id(self, mock_hook, fetch_cluster_info_mock, mock_gke_conn):
        gke_op = GKEDescribeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            job_name=TASK_NAME,
            namespace=NAMESPACE,
            gcp_conn_id="test_conn",
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"


class TestGKECreateCustomResourceOperator:
    def setup_method(self):
        self.gke_op = GKECreateCustomResourceOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            yaml_conf=VALID_RESOURCE_YAML,
        )

    def test_template_fields(self):
        assert set(KubernetesCreateResourceOperator.template_fields).issubset(
            GKECreateCustomResourceOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_CREATE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute(self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_CREATE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_CREATE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()


class TestGKEDeleteCustomResourceOperator:
    def setup_method(self):
        self.gke_op = GKEDeleteCustomResourceOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            yaml_conf=VALID_RESOURCE_YAML,
        )

    def test_template_fields(self):
        assert set(KubernetesDeleteResourceOperator.template_fields).issubset(
            GKEDeleteCustomResourceOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_DELETE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute(self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_DELETE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_DELETE_RES_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()


class TestGKEStartKueueJobOperator:
    def setup_method(self):
        self.gke_op = GKEStartKueueJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            queue_name=QUEUE_NAME,
        )
        self.gke_op.job = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_template_fields(self):
        assert set(GKEStartJobOperator.template_fields).issubset(
            GKEStartKueueJobOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute(self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEStartKueueJobOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                image=IMAGE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    def test_default_gcp_conn_id(self):
        gke_op = GKEStartKueueJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            queue_name=QUEUE_NAME,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    def test_gcp_conn_id(self, get_con_mock):
        gke_op = GKEStartKueueJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            gcp_conn_id="test_conn",
            queue_name=QUEUE_NAME,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"


class TestGKEDeleteJobOperator:
    def setup_method(self):
        self.gke_op = GKEDeleteJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_template_fields(self):
        assert set(KubernetesDeleteJobOperator.template_fields).issubset(
            GKEDeleteJobOperator.template_fields
        )

    @mock.patch.dict(os.environ, {})
    @mock.patch(DEL_KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute(self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEDeleteJobOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(DEL_KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(DEL_KUB_JOB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_hook, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    def test_default_gcp_conn_id(self):
        gke_op = GKEDeleteJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    def test_gcp_conn_id(self, get_con_mock):
        gke_op = GKEDeleteJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            gcp_conn_id="test_conn",
        )
        gke_op._cluster_url = CLUSTER_URL
        gke_op._ssl_ca_cert = SSL_CA_CERT
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"


class TestGKESuspendJobOperator:
    def setup_method(self):
        self.gke_op = GKESuspendJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKESuspendJobOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute(self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    def test_default_gcp_conn_id(self, fetch_cluster_info_mock):
        gke_op = GKESuspendJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_gcp_conn_id(self, mock_hook, fetch_cluster_info_mock, mock_gke_conn):
        gke_op = GKESuspendJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            gcp_conn_id="test_conn",
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"


class TestGKEResumeJobOperator:
    def setup_method(self):
        self.gke_op = GKEResumeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEResumeJobOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute(self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute_with_impersonation_service_account(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        mock_job_hook.return_value.get_job.return_value = mock.MagicMock()
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=[
            Connection(
                extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'})
            )
        ],
    )
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(GKE_K8S_HOOK_PATH)
    def test_execute_with_impersonation_service_chain_one_element(
        self, mock_job_hook, mock_hook, fetch_cluster_info_mock, file_mock, get_con_mock
    ):
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()

    @pytest.mark.db_test
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    def test_default_gcp_conn_id(self, fetch_cluster_info_mock):
        gke_op = GKEResumeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "google_cloud_default"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
        return_value=Connection(conn_id="test_conn"),
    )
    @mock.patch(f"{GKE_CLUSTER_AUTH_DETAILS_PATH}.fetch_cluster_info")
    @mock.patch(GKE_HOOK_PATH)
    def test_gcp_conn_id(self, mock_hook, fetch_cluster_info_mock, mock_gke_conn):
        gke_op = GKEResumeJobOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            gcp_conn_id="test_conn",
        )
        fetch_cluster_info_mock.return_value = (CLUSTER_URL, SSL_CA_CERT)
        hook = gke_op.hook

        assert hook.gcp_conn_id == "test_conn"
