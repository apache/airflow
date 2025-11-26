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

import copy
import logging
from asyncio import Future
from unittest import mock

import kubernetes.client
import pytest
import pytest_asyncio
from google.cloud.container_v1 import ClusterManagerAsyncClient
from google.cloud.container_v1.types import Cluster

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.kubernetes_engine import (
    GKEAsyncHook,
    GKEHook,
    GKEKubernetesAsyncHook,
    GKEKubernetesHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

TASK_ID = "test-gke-cluster-operator"
CLUSTER_NAME = "test-cluster"
NAMESPACE = "test-cluster-namespace"
TEST_GCP_PROJECT_ID = "test-project"
GKE_ZONE = "test-zone"
BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
GKE_STRING = "airflow.providers.google.cloud.hooks.kubernetes_engine.{}"
K8S_HOOK = "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook"
CLUSTER_URL = "https://path.to.cluster"
SSL_CA_CERT = "test-ssl-ca-cert"
POD_NAME = "test-pod-name"
POD_NAMESPACE = "test"
ASYNC_HOOK_STRING = GKE_STRING.format("GKEAsyncHook")
GCP_CONN_ID = "test-gcp-conn-id"
IMPERSONATE_CHAIN = ["impersonate", "this", "test"]
OPERATION_NAME = "test-operation-name"
CLUSTER_TEST_AUTOPILOT = {
    "name": "autopilot-cluster",
    "initial_node_count": 1,
    "autopilot": {
        "enabled": True,
    },
    "autoscaling": {
        "enable_node_autoprovisioning": False,
    },
    "node_pools": [
        {
            "name": "pool",
            "config": {"machine_type": "e2-standard-32", "disk_size_gb": 11},
            "initial_node_count": 2,
        }
    ],
}
CLUSTER_TEST_AUTOPROVISIONING = {
    "name": "cluster_autoprovisioning",
    "initial_node_count": 1,
    "autopilot": {
        "enabled": False,
    },
    "node_pools": [
        {
            "name": "pool",
            "config": {"machine_type": "e2-standard-32", "disk_size_gb": 11},
            "initial_node_count": 2,
        }
    ],
    "autoscaling": {
        "enable_node_autoprovisioning": True,
        "resource_limits": [
            {"resource_type": "cpu", "maximum": 1000000000},
            {"resource_type": "memory", "maximum": 1000000000},
        ],
    },
}
CLUSTER_TEST_AUTOSCALED = {
    "name": "autoscaled_cluster",
    "autopilot": {
        "enabled": False,
    },
    "node_pools": [
        {
            "name": "autoscaled-pool",
            "config": {"machine_type": "e2-standard-32", "disk_size_gb": 11},
            "initial_node_count": 2,
            "autoscaling": {
                "enabled": True,
                "max_node_count": 10,
            },
        }
    ],
    "autoscaling": {
        "enable_node_autoprovisioning": False,
    },
}

CLUSTER_TEST_REGULAR = {
    "name": "regular_cluster",
    "initial_node_count": 1,
    "autopilot": {
        "enabled": False,
    },
    "node_pools": [
        {
            "name": "autoscaled-pool",
            "config": {"machine_type": "e2-standard-32", "disk_size_gb": 11},
            "initial_node_count": 2,
            "autoscaling": {
                "enabled": False,
            },
        }
    ],
    "autoscaling": {
        "enable_node_autoprovisioning": False,
    },
}
pods = {
    "succeeded": {
        "metadata": {"name": "test-pod", "namespace": "default"},
        "status": {"phase": "Succeeded"},
    },
    "pending": {
        "metadata": {"name": "test-pod", "namespace": "default"},
        "status": {"phase": "Pending"},
    },
    "running": {
        "metadata": {"name": "test-pod", "namespace": "default"},
        "status": {"phase": "Running"},
    },
}


@pytest.mark.db_test
class TestGKEHookClient:
    def setup_method(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)

    @mock.patch(GKE_STRING.format("GKEHook.get_credentials"))
    @mock.patch(GKE_STRING.format("ClusterManagerClient"))
    def test_gke_cluster_client_creation(self, mock_client, mock_get_creds):
        result = self.gke_hook.get_cluster_manager_client()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.gke_hook._client == result


class TestGKEHookDelete:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEHook(gcp_conn_id="test", location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_delete_cluster(self, wait_mock):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_delete = self.gke_hook._client.delete_cluster = mock.Mock()

        self.gke_hook.delete_cluster(
            name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID, retry=retry_mock, timeout=timeout_mock
        )

        client_delete.assert_called_once_with(
            name=f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}/clusters/{CLUSTER_NAME}",
            retry=retry_mock,
            timeout=timeout_mock,
        )
        wait_mock.assert_called_once_with(client_delete.return_value, TEST_GCP_PROJECT_ID)

    @mock.patch(GKE_STRING.format("GKEHook.log"))
    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_delete_cluster_not_found(self, wait_mock, log_mock):
        from google.api_core.exceptions import NotFound

        # To force an error
        message = "Not Found"
        self.gke_hook._client.delete_cluster.side_effect = NotFound(message=message)

        self.gke_hook.delete_cluster(name="not-existing", project_id=TEST_GCP_PROJECT_ID)
        wait_mock.assert_not_called()
        log_mock.info.assert_any_call("Assuming Success: %s", message)

    @mock.patch(
        BASE_STRING.format("GoogleBaseHook.project_id"),
        new_callable=mock.PropertyMock,
        return_value=None,
    )
    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_delete_cluster_error(self, wait_mock, mock_project_id):
        # To force an error
        self.gke_hook._client.delete_cluster.side_effect = AirflowException("400")
        with pytest.raises(AirflowException):
            self.gke_hook.delete_cluster(name="a-cluster")
        wait_mock.assert_not_called()


class TestGKEHookCreate:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEHook(gcp_conn_id="test", location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_create_cluster_proto(self, wait_mock):
        mock_cluster_proto = Cluster()
        mock_cluster_proto.name = CLUSTER_NAME

        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()

        self.gke_hook.create_cluster(
            cluster=mock_cluster_proto, project_id=TEST_GCP_PROJECT_ID, retry=retry_mock, timeout=timeout_mock
        )

        client_create.assert_called_once_with(
            parent=f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}",
            cluster=mock_cluster_proto,
            retry=retry_mock,
            timeout=timeout_mock,
        )
        wait_mock.assert_called_once_with(client_create.return_value, TEST_GCP_PROJECT_ID)

    @mock.patch(GKE_STRING.format("Cluster.from_json"))
    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_create_cluster_dict(self, wait_mock, convert_mock):
        mock_cluster_dict = {"name": CLUSTER_NAME}
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()
        proto_mock = convert_mock.return_value = mock.Mock()

        self.gke_hook.create_cluster(
            cluster=mock_cluster_dict, project_id=TEST_GCP_PROJECT_ID, retry=retry_mock, timeout=timeout_mock
        )

        client_create.assert_called_once_with(
            parent=f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}",
            cluster=proto_mock,
            retry=retry_mock,
            timeout=timeout_mock,
        )
        wait_mock.assert_called_once_with(client_create.return_value, TEST_GCP_PROJECT_ID)

    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_create_cluster_error(self, wait_mock):
        # to force an error
        mock_cluster_proto = None

        with pytest.raises(AirflowException):
            self.gke_hook.create_cluster(mock_cluster_proto)
        wait_mock.assert_not_called()

    @mock.patch(GKE_STRING.format("GKEHook.log"))
    @mock.patch(GKE_STRING.format("GKEHook.wait_for_operation"))
    def test_create_cluster_already_exists(self, wait_mock, log_mock):
        from google.api_core.exceptions import AlreadyExists

        # To force an error
        message = "Already Exists"
        self.gke_hook._client.create_cluster.side_effect = AlreadyExists(message=message)

        with pytest.raises(AlreadyExists):
            self.gke_hook.create_cluster(cluster={}, project_id=TEST_GCP_PROJECT_ID)
        wait_mock.assert_not_called()


class TestGKEHookGet:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEHook(gcp_conn_id="test", location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    def test_get_cluster(self):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_get = self.gke_hook._client.get_cluster = mock.Mock()

        self.gke_hook.get_cluster(
            name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID, retry=retry_mock, timeout=timeout_mock
        )

        client_get.assert_called_once_with(
            name=f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}/clusters/{CLUSTER_NAME}",
            retry=retry_mock,
            timeout=timeout_mock,
        )


class TestGKEHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEHook(gcp_conn_id="test", location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch(GKE_STRING.format("ClusterManagerClient"))
    @mock.patch(GKE_STRING.format("GKEHook.get_credentials"))
    def test_get_client(self, mock_get_credentials, mock_client):
        self.gke_hook._client = None
        self.gke_hook.get_cluster_manager_client()
        assert mock_get_credentials.called
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value, client_info=CLIENT_INFO
        )

    def test_get_operation(self):
        self.gke_hook._client.get_operation = mock.Mock()
        self.gke_hook.get_operation("TEST_OP", project_id=TEST_GCP_PROJECT_ID)
        self.gke_hook._client.get_operation.assert_called_once_with(
            name=f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}/operations/TEST_OP"
        )

    def test_append_label(self):
        key = "test-key"
        val = "test-val"
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_once_with({key: val})

    def test_append_label_replace(self):
        key = "test-key"
        val = "test.val+this"
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_once_with({key: "test-val-this"})

    @mock.patch(GKE_STRING.format("time.sleep"))
    def test_wait_for_response_done(self, time_mock):
        from google.cloud.container_v1.types import Operation

        mock_op = mock.Mock()
        mock_op.status = Operation.Status.DONE
        self.gke_hook.wait_for_operation(mock_op)
        assert time_mock.call_count == 1

    @mock.patch(GKE_STRING.format("time.sleep"))
    def test_wait_for_response_exception(self, time_mock):
        from google.cloud.container_v1.types import Operation
        from google.cloud.exceptions import GoogleCloudError

        mock_op = mock.Mock()
        mock_op.status = Operation.Status.ABORTING

        with pytest.raises(GoogleCloudError):
            self.gke_hook.wait_for_operation(mock_op)
        assert time_mock.call_count == 1

    @mock.patch(GKE_STRING.format("GKEHook.get_operation"))
    @mock.patch(GKE_STRING.format("time.sleep"))
    def test_wait_for_response_running(self, time_mock, operation_mock):
        from google.cloud.container_v1.types import Operation

        running_op, done_op, pending_op = mock.Mock(), mock.Mock(), mock.Mock()
        running_op.status = Operation.Status.RUNNING
        done_op.status = Operation.Status.DONE
        pending_op.status = Operation.Status.PENDING

        # Status goes from Running -> Pending -> Done
        operation_mock.side_effect = [pending_op, done_op]
        self.gke_hook.wait_for_operation(running_op, project_id=TEST_GCP_PROJECT_ID)

        assert time_mock.call_count == 3
        operation_mock.assert_any_call(running_op.name, project_id=TEST_GCP_PROJECT_ID)
        operation_mock.assert_any_call(pending_op.name, project_id=TEST_GCP_PROJECT_ID)
        assert operation_mock.call_count == 2

    @pytest.mark.parametrize(
        ("cluster_obj", "expected_result"),
        [
            (CLUSTER_TEST_AUTOPROVISIONING, True),
            (CLUSTER_TEST_AUTOSCALED, True),
            (CLUSTER_TEST_AUTOPILOT, True),
            (CLUSTER_TEST_REGULAR, False),
        ],
    )
    def test_check_cluster_autoscaling_ability(self, cluster_obj, expected_result):
        result = self.gke_hook.check_cluster_autoscaling_ability(cluster_obj)
        assert result == expected_result


class TestGKEKubernetesHookDeployments:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEKubernetesHook(gcp_conn_id="test", ssl_ca_cert=None, cluster_url=None)
        self.gke_hook._client = mock.Mock()

        def refresh_token(request):
            self.credentials.token = "New"

        self.credentials = mock.MagicMock()
        self.credentials.token = "Old"
        self.credentials.expired = False
        self.credentials.refresh = refresh_token

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_invalid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = True
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "New"

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_valid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = False
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "Old"

    def _get_config(self):
        return kubernetes.client.configuration.Configuration()

    def _get_credentials(self):
        return self.credentials

    @pytest.mark.parametrize(
        ("api_client", "expected_client"),
        [
            (None, mock.MagicMock()),
            (mock_client := mock.MagicMock(), mock_client),  # type: ignore[name-defined]
        ],
    )
    @mock.patch(GKE_STRING.format("super"))
    @mock.patch(GKE_STRING.format("GKEKubernetesHook.get_conn"))
    def test_apply_from_yaml_file(self, mock_get_conn, mock_super, api_client, expected_client):
        kwargs = dict(
            api_client=api_client,
            yaml_file=mock.MagicMock(),
            yaml_objects=mock.MagicMock(),
            verbose=mock.MagicMock(),
            namespace=mock.MagicMock(),
        )
        expected_kwargs = copy.deepcopy(kwargs)
        expected_kwargs["api_client"] = expected_client
        mock_get_conn.return_value = expected_client

        self.gke_hook.apply_from_yaml_file(**kwargs)

        if api_client is None:
            mock_get_conn.assert_called_once()
        mock_super.return_value.apply_from_yaml_file.assert_called_once_with(**expected_kwargs)


class TestGKEKubernetesAsyncHook:
    @staticmethod
    def make_mock_awaitable(mock_obj, result=None):
        f = Future()
        f.set_result(result)

        mock_obj.return_value = f

    @pytest.fixture
    def async_hook(self):
        return GKEKubernetesAsyncHook(
            cluster_url=CLUSTER_URL,
            ssl_ca_cert=SSL_CA_CERT,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATE_CHAIN,
        )

    @pytest.mark.asyncio
    @mock.patch(GKE_STRING.format("GKEKubernetesAsyncHook.get_conn"))
    @mock.patch(GKE_STRING.format("async_client.CoreV1Api.read_namespaced_pod"))
    async def test_get_pod(self, read_namespace_pod_mock, get_conn_mock, async_hook):
        self.make_mock_awaitable(read_namespace_pod_mock)

        await async_hook.get_pod(name=POD_NAME, namespace=POD_NAMESPACE)

        get_conn_mock.assert_called_once_with()
        read_namespace_pod_mock.assert_called_with(
            name=POD_NAME,
            namespace=POD_NAMESPACE,
        )

    @pytest.mark.asyncio
    @mock.patch(GKE_STRING.format("GKEKubernetesAsyncHook.get_conn"))
    @mock.patch(GKE_STRING.format("async_client.CoreV1Api.delete_namespaced_pod"))
    async def test_delete_pod(self, delete_namespaced_pod, get_conn_mock, async_hook):
        self.make_mock_awaitable(delete_namespaced_pod)

        await async_hook.delete_pod(name=POD_NAME, namespace=POD_NAMESPACE)

        get_conn_mock.assert_called_once_with()
        delete_namespaced_pod.assert_called_with(
            name=POD_NAME,
            namespace=POD_NAMESPACE,
            body=kubernetes.client.V1DeleteOptions(),
        )

    @pytest.mark.asyncio
    @mock.patch(GKE_STRING.format("GKEKubernetesAsyncHook.get_conn"))
    @mock.patch(GKE_STRING.format("async_client.CoreV1Api.read_namespaced_pod_log"))
    async def test_read_logs(self, read_namespaced_pod_log, get_conn_mock, async_hook, caplog):
        caplog.set_level(logging.INFO)
        self.make_mock_awaitable(read_namespaced_pod_log, result="Test string #1\nTest string #2\n")

        logs = await async_hook.read_logs(name=POD_NAME, namespace=POD_NAMESPACE)

        get_conn_mock.assert_called_once_with()
        read_namespaced_pod_log.assert_called_with(
            name=POD_NAME,
            namespace=POD_NAMESPACE,
            follow=False,
            timestamps=True,
            container=None,
            since_seconds=None,
        )
        assert "Test string #1" in logs
        assert "Test string #2" in logs


@pytest_asyncio.fixture
async def async_gke_hook():
    return GKEAsyncHook(
        gcp_conn_id=GCP_CONN_ID,
        location=GKE_ZONE,
        impersonation_chain=IMPERSONATE_CHAIN,
    )


@pytest_asyncio.fixture
async def mock_async_gke_cluster_client():
    f = Future()
    f.set_result(None)
    client = mock.MagicMock(spec=ClusterManagerAsyncClient)
    client.get_operation.return_value = f
    return client


class TestGKEAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(f"{ASYNC_HOOK_STRING}._get_client")
    async def test_get_operation(self, mock_get_client, async_gke_hook, mock_async_gke_cluster_client):
        mock_get_client.return_value = mock_async_gke_cluster_client

        await async_gke_hook.get_operation(
            operation_name=OPERATION_NAME,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operation_path = f"projects/{TEST_GCP_PROJECT_ID}/locations/{GKE_ZONE}/operations/{OPERATION_NAME}"
        mock_async_gke_cluster_client.get_operation.assert_called_once_with(
            name=operation_path,
        )


class TestGKEKubernetesHookPod:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEKubernetesHook(
                gcp_conn_id="test",
                impersonation_chain=IMPERSONATE_CHAIN,
                ssl_ca_cert=None,
                cluster_url=None,
            )
        self.gke_hook._client = mock.Mock()

        def refresh_token(request):
            self.credentials.token = "New"

        self.credentials = mock.MagicMock()
        self.credentials.token = "Old"
        self.credentials.expired = False
        self.credentials.refresh = refresh_token

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_invalid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = True
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "New"

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_valid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = False
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "Old"

    def _get_config(self):
        return kubernetes.client.configuration.Configuration()

    def _get_credentials(self):
        return self.credentials

    @pytest.mark.parametrize(
        ("disable_tcp_keepalive", "expected"),
        (
            (True, False),
            (None, True),
            (False, True),
        ),
    )
    @mock.patch(GKE_STRING.format("_enable_tcp_keepalive"))
    def test_disable_tcp_keepalive(
        self,
        mock_enable,
        disable_tcp_keepalive,
        expected,
    ):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            gke_hook = GKEKubernetesHook(
                gcp_conn_id="test",
                impersonation_chain=IMPERSONATE_CHAIN,
                ssl_ca_cert=None,
                cluster_url=None,
                enable_tcp_keepalive=not disable_tcp_keepalive,
            )
        gke_hook.get_credentials = self._get_credentials

        api_conn = gke_hook.get_conn()
        assert mock_enable.called is expected
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)


class TestGKEKubernetesHookJob:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEKubernetesHook(gcp_conn_id="test", ssl_ca_cert=None, cluster_url=None)
        self.gke_hook._client = mock.Mock()

        def refresh_token(request):
            self.credentials.token = "New"

        self.credentials = mock.MagicMock()
        self.credentials.token = "Old"
        self.credentials.expired = False
        self.credentials.refresh = refresh_token

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_invalid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = True
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "New"

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_valid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = False
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "Old"

    def _get_config(self):
        return kubernetes.client.configuration.Configuration()

    def _get_credentials(self):
        return self.credentials


class TestGKEKubernetesHookCustomResources:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gke_hook = GKEKubernetesHook(gcp_conn_id="test", ssl_ca_cert=None, cluster_url=None)
        self.gke_hook._client = mock.Mock()

        def refresh_token(request):
            self.credentials.token = "New"

        self.credentials = mock.MagicMock()
        self.credentials.token = "Old"
        self.credentials.expired = False
        self.credentials.refresh = refresh_token

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_invalid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = True
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "New"

    @mock.patch(GKE_STRING.format("google_requests.Request"))
    def test_get_connection_update_hook_with_valid_token(self, mock_request):
        self.gke_hook._get_config = self._get_config
        self.gke_hook.get_credentials = self._get_credentials
        self.gke_hook.get_credentials().expired = False
        the_client: kubernetes.client.ApiClient = self.gke_hook.get_conn()

        the_client.configuration.refresh_api_key_hook(the_client.configuration)

        assert self.gke_hook.get_credentials().token == "Old"

    def _get_config(self):
        return kubernetes.client.configuration.Configuration()

    def _get_credentials(self):
        return self.credentials
