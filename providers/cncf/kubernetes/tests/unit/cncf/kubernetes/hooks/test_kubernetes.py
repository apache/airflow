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
import tempfile
from asyncio import Future
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import anyio
import kubernetes
import pytest
import pytest_asyncio
import yaml
from kubernetes.client import V1Deployment, V1DeploymentStatus
from kubernetes.client.rest import ApiException
from kubernetes.config import ConfigException

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook, KubernetesHook

from tests_common.test_utils.db import clear_test_connections
from tests_common.test_utils.providers import get_provider_min_airflow_version

pytestmark = pytest.mark.db_test

KUBE_CONFIG_PATH = os.getenv("KUBECONFIG", "~/.kube/config")
HOOK_MODULE = "airflow.providers.cncf.kubernetes.hooks.kubernetes"

CONN_ID = "kubernetes-test-id"
ASYNC_CONFIG_PATH = "/files/path/to/config/file"
POD_NAME = "test-pod"
NAMESPACE = "test-namespace"
JOB_NAME = "test-job"
CONTAINER_NAME = "test-container"
POLL_INTERVAL = 100
YAML_URL = "https://test-yaml-url.com"

NOT_READY_DEPLOYMENT = V1Deployment(
    status=V1DeploymentStatus(
        observed_generation=1,
        ready_replicas=None,
        replicas=None,
        unavailable_replicas=1,
        updated_replicas=None,
    )
)
READY_DEPLOYMENT = V1Deployment(
    status=V1DeploymentStatus(
        observed_generation=1, ready_replicas=1, replicas=1, unavailable_replicas=None, updated_replicas=1
    )
)
DEPLOYMENT_NAME = "test-deployment-name"


class DeprecationRemovalRequired(AirflowException): ...


DEFAULT_CONN_ID = "kubernetes_default"


@pytest.fixture
def remove_default_conn(monkeypatch):
    original_env_var = os.environ.get(f"AIRFLOW_CONN_{DEFAULT_CONN_ID.upper()}")

    # remove the env variable to simulate no default connection
    if original_env_var:
        monkeypatch.delenv(f"AIRFLOW_CONN_{DEFAULT_CONN_ID.upper()}")

    yield

    # restore the original env variable
    if original_env_var:
        monkeypatch.setenv(f"AIRFLOW_CONN_{DEFAULT_CONN_ID.upper()}", original_env_var)


class TestKubernetesHook:
    # TODO: Potential performance issue, converted setup_class to a setup_connections function level fixture
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        """Create test connections for Kubernetes hook tests."""
        import json

        connections = [
            ("in_cluster", {"in_cluster": True}),
            ("in_cluster_empty", {"in_cluster": ""}),
            ("kube_config", {"kube_config": '{"test": "kube"}'}),
            ("kube_config_dict", {"kube_config": {"test": "kube"}}),
            ("kube_config_path", {"kube_config_path": "path/to/file"}),
            ("kube_config_empty", {"kube_config": ""}),
            ("kube_config_path_empty", {"kube_config_path": ""}),
            ("kube_config_empty", {"kube_config": ""}),
            ("kube_config_path_empty", {"kube_config_path": ""}),
            ("context_empty", {"cluster_context": ""}),
            ("context", {"cluster_context": "my-context"}),
            ("with_namespace", {"namespace": "mock_namespace"}),
            ("default_kube_config", {}),
            ("disable_verify_ssl", {"disable_verify_ssl": True}),
            ("disable_verify_ssl_empty", {"disable_verify_ssl": ""}),
            ("disable_tcp_keepalive", {"disable_tcp_keepalive": True}),
            ("disable_tcp_keepalive_empty", {"disable_tcp_keepalive": ""}),
            ("sidecar_container_image", {"xcom_sidecar_container_image": "private.repo.com/alpine:3.16"}),
            ("sidecar_container_image_empty", {"xcom_sidecar_container_image": ""}),
            (
                "sidecar_container_resources",
                {
                    "xcom_sidecar_container_resources": json.dumps(
                        {
                            "requests": {"cpu": "1m", "memory": "10Mi"},
                            "limits": {"cpu": "1m", "memory": "50Mi"},
                        }
                    ),
                },
            ),
            ("sidecar_container_resources_empty", {"xcom_sidecar_container_resources": ""}),
        ]
        for conn_id, extra in connections:
            create_connection_without_db(
                Connection(conn_type="kubernetes", conn_id=conn_id, extra=json.dumps(extra))
            )

    @classmethod
    def teardown_class(cls) -> None:
        clear_test_connections()

    @pytest.mark.parametrize(
        ("in_cluster_param", "conn_id", "in_cluster_called"),
        (
            (True, None, True),
            (None, None, False),
            (False, None, False),
            (None, "in_cluster", True),
            (True, "in_cluster", True),
            (False, "in_cluster", False),
            (None, "in_cluster_empty", False),
            (True, "in_cluster_empty", True),
            (False, "in_cluster_empty", False),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client")
    def test_in_cluster_connection(
        self,
        mock_get_default_client,
        mock_in_cluster_loader,
        mock_merger,
        mock_loader,
        in_cluster_param,
        conn_id,
        in_cluster_called,
    ):
        """
        Verifies whether in_cluster is called depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, in_cluster=in_cluster_param)
        mock_get_default_client.return_value = kubernetes.client.api_client.ApiClient()
        api_conn = kubernetes_hook.get_conn()
        if in_cluster_called:
            mock_in_cluster_loader.assert_called_once()
            mock_merger.assert_not_called()
            mock_loader.assert_not_called()
        else:
            mock_get_default_client.assert_called()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)
        if not mock_get_default_client.called:
            # get_default_client is mocked, so only check is_in_cluster if it isn't called
            assert kubernetes_hook.is_in_cluster is in_cluster_called

    @pytest.mark.parametrize("in_cluster_fails", [True, False])
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    def test_get_default_client(
        self,
        mock_incluster,
        mock_merger,
        mock_loader,
        in_cluster_fails,
    ):
        """
        Verifies the behavior of the ``_get_default_client`` function.  It should try the "in cluster"
        loader first but if that fails, try to use the default kubeconfig file.
        """
        if in_cluster_fails:
            mock_incluster.side_effect = ConfigException("any")
        kubernetes_hook = KubernetesHook()
        api_conn = kubernetes_hook._get_default_client()
        if in_cluster_fails:
            mock_incluster.assert_called_once()
            mock_merger.assert_called_once_with(KUBE_CONFIG_PATH)
            mock_loader.assert_called_once()
            assert kubernetes_hook.is_in_cluster is False
        else:
            mock_incluster.assert_called_once()
            mock_merger.assert_not_called()
            mock_loader.assert_not_called()
            assert kubernetes_hook.is_in_cluster is True
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("disable_verify_ssl", "conn_id", "disable_called"),
        (
            (True, None, True),
            (None, None, False),
            (False, None, False),
            (None, "disable_verify_ssl", True),
            (True, "disable_verify_ssl", True),
            (False, "disable_verify_ssl", False),
            (None, "disable_verify_ssl_empty", False),
            (True, "disable_verify_ssl_empty", True),
            (False, "disable_verify_ssl_empty", False),
        ),
    )
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader", new=MagicMock())
    @patch(f"{HOOK_MODULE}._disable_verify_ssl")
    def test_disable_verify_ssl(
        self,
        mock_disable,
        disable_verify_ssl,
        conn_id,
        disable_called,
    ):
        """
        Verifies whether disable verify ssl is called depending on combination of hook param and
        connection extra. Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, disable_verify_ssl=disable_verify_ssl)
        api_conn = kubernetes_hook.get_conn()
        assert mock_disable.called is disable_called
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("disable_tcp_keepalive", "conn_id", "expected"),
        (
            (True, None, False),
            (None, None, True),
            (False, None, True),
            (None, "disable_tcp_keepalive", False),
            (True, "disable_tcp_keepalive", False),
            (False, "disable_tcp_keepalive", True),
            (None, "disable_tcp_keepalive_empty", True),
            (True, "disable_tcp_keepalive_empty", False),
            (False, "disable_tcp_keepalive_empty", True),
        ),
    )
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader", new=MagicMock())
    @patch(f"{HOOK_MODULE}._enable_tcp_keepalive")
    def test_disable_tcp_keepalive(
        self,
        mock_enable,
        disable_tcp_keepalive,
        conn_id,
        expected,
    ):
        """
        Verifies whether enable tcp keepalive is called depending on combination of hook
        param and connection extra. Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, disable_tcp_keepalive=disable_tcp_keepalive)
        api_conn = kubernetes_hook.get_conn()
        assert mock_enable.called is expected
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("config_path_param", "conn_id", "call_path"),
        (
            (None, None, KUBE_CONFIG_PATH),
            ("/my/path/override", None, "/my/path/override"),
            (None, "kube_config_path", "path/to/file"),
            ("/my/path/override", "kube_config_path", "/my/path/override"),
            (None, "kube_config_path_empty", KUBE_CONFIG_PATH),
            ("/my/path/override", "kube_config_path_empty", "/my/path/override"),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_kube_config_path(
        self, mock_kube_config_merger, mock_kube_config_loader, config_path_param, conn_id, call_path
    ):
        """
        Verifies kube config path depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, config_file=config_path_param)
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_merger.assert_called_once_with(call_path)
        mock_kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("conn_id", "has_config"),
        (
            (None, False),
            ("kube_config", True),
            ("kube_config_dict", True),
            ("kube_config_empty", False),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch.object(tempfile, "NamedTemporaryFile")
    def test_kube_config_connection(
        self, mock_tempfile, mock_kube_config_merger, mock_kube_config_loader, conn_id, has_config
    ):
        """
        Verifies whether temporary kube config file is created.
        """
        mock_tempfile.return_value.__enter__.return_value.name = "fake-temp-file"
        mock_kube_config_merger.return_value.config = {"fake_config": "value"}
        kubernetes_hook = KubernetesHook(conn_id=conn_id)
        api_conn = kubernetes_hook.get_conn()
        if has_config:
            mock_tempfile.is_called_once()
            mock_kube_config_loader.assert_called_once()
            mock_kube_config_merger.assert_called_once_with("fake-temp-file")
        else:
            mock_tempfile.assert_not_called()
            mock_kube_config_loader.assert_called_once()
            mock_kube_config_merger.assert_called_once_with(KUBE_CONFIG_PATH)
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("context_param", "conn_id", "expected_context"),
        (
            ("param-context", None, "param-context"),
            (None, None, None),
            ("param-context", "context", "param-context"),
            (None, "context", "my-context"),
            ("param-context", "context_empty", "param-context"),
            (None, "context_empty", None),
        ),
    )
    @patch("kubernetes.config.load_kube_config")
    def test_cluster_context(self, mock_load_kube_config, context_param, conn_id, expected_context):
        """
        Verifies cluster context depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, cluster_context=context_param)
        kubernetes_hook.get_conn()
        mock_load_kube_config.assert_called_with(client_configuration=None, context=expected_context)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION", "/mock/config")
    def test_default_kube_config_connection(self, mock_kube_config_merger, mock_kube_config_loader):
        kubernetes_hook = KubernetesHook(conn_id="default_kube_config")
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_merger.assert_called_once_with("/mock/config")
        mock_kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        ("conn_id", "expected"),
        (
            pytest.param(None, None, id="no-conn-id"),
            pytest.param("with_namespace", "mock_namespace", id="conn-with-namespace"),
            pytest.param("default_kube_config", None, id="conn-without-namespace"),
        ),
    )
    def test_get_namespace(self, conn_id, expected):
        hook = KubernetesHook(conn_id=conn_id)
        assert hook.get_namespace() == expected
        if get_provider_min_airflow_version("apache-airflow-providers-cncf-kubernetes") >= (6, 0):
            raise DeprecationRemovalRequired(
                "You must update get_namespace so that if namespace not set "
                "in the connection, then None is returned. To do so, remove get_namespace "
                "and rename _get_namespace to get_namespace."
            )

    @pytest.mark.parametrize(
        ("conn_id", "expected"),
        (
            pytest.param("sidecar_container_image", "private.repo.com/alpine:3.16", id="sidecar-with-image"),
            pytest.param("sidecar_container_image_empty", None, id="sidecar-without-image"),
        ),
    )
    def test_get_xcom_sidecar_container_image(self, conn_id, expected):
        hook = KubernetesHook(conn_id=conn_id)
        assert hook.get_xcom_sidecar_container_image() == expected

    @pytest.mark.parametrize(
        ("conn_id", "expected"),
        (
            pytest.param(
                "sidecar_container_resources",
                {
                    "requests": {"cpu": "1m", "memory": "10Mi"},
                    "limits": {
                        "cpu": "1m",
                        "memory": "50Mi",
                    },
                },
                id="sidecar-with-resources",
            ),
            pytest.param("sidecar_container_resources_empty", None, id="sidecar-without-resources"),
        ),
    )
    def test_get_xcom_sidecar_container_resources(self, conn_id, expected):
        hook = KubernetesHook(conn_id=conn_id)
        assert hook.get_xcom_sidecar_container_resources() == expected

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_client_types(self, mock_kube_config_merger, mock_kube_config_loader):
        hook = KubernetesHook(None)
        assert isinstance(hook.core_v1_client, kubernetes.client.CoreV1Api)
        assert isinstance(hook.api_client, kubernetes.client.ApiClient)
        assert isinstance(hook.get_conn(), kubernetes.client.ApiClient)

    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client")
    def test_prefixed_names_still_work(self, mock_get_client):
        conn_uri = "kubernetes://?extra__kubernetes__cluster_context=test&extra__kubernetes__namespace=test"
        with mock.patch.dict("os.environ", AIRFLOW_CONN_KUBERNETES_DEFAULT=conn_uri):
            kubernetes_hook = KubernetesHook(conn_id="kubernetes_default")
            kubernetes_hook.get_conn()
            mock_get_client.assert_called_with(cluster_context="test")
            assert kubernetes_hook.get_namespace() == "test"

    def test_missing_default_connection_is_ok(self, remove_default_conn, sdk_connection_not_found):
        # prove to ourselves that the default conn doesn't exist
        k8s_conn_exists = os.environ.get(f"AIRFLOW_CONN_{DEFAULT_CONN_ID.upper()}")
        assert k8s_conn_exists is None
        # verify K8sHook still works
        hook = KubernetesHook()
        assert hook.conn_extras == {}

        # meanwhile, asking for non-default should still fail if it doesn't exist
        hook = KubernetesHook("some_conn")
        with pytest.raises(AirflowNotFoundException, match="The conn_id `some_conn` isn't defined"):
            hook.conn_extras

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch(f"{HOOK_MODULE}.client.CustomObjectsApi")
    def test_delete_custom_object(
        self, mock_custom_object_api, mock_kube_config_merger, mock_kube_config_loader
    ):
        hook = KubernetesHook()
        hook.delete_custom_object(
            group="group",
            version="version",
            plural="plural",
            name="name",
            namespace="namespace",
            _preload_content="_preload_content",
        )

        mock_custom_object_api.return_value.delete_namespaced_custom_object.assert_called_once_with(
            group="group",
            version="version",
            plural="plural",
            name="name",
            namespace="namespace",
            _preload_content="_preload_content",
        )

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch(f"{HOOK_MODULE}.KubernetesHook.batch_v1_client")
    def test_get_job_status(self, mock_client, mock_kube_config_merger, mock_kube_config_loader):
        job_expected = mock_client.read_namespaced_job_status.return_value

        hook = KubernetesHook()
        job_actual = hook.get_job_status(job_name=JOB_NAME, namespace=NAMESPACE)

        mock_client.read_namespaced_job_status.assert_called_once_with(
            name=JOB_NAME, namespace=NAMESPACE, pretty=True
        )
        assert job_actual == job_expected

    @pytest.mark.parametrize(
        ("conditions", "expected_result"),
        [
            (None, False),
            ([], False),
            ([mock.MagicMock(type="Complete", status=True)], False),
            ([mock.MagicMock(type="Complete", status=False)], False),
            ([mock.MagicMock(type="Failed", status=False)], False),
            ([mock.MagicMock(type="Failed", status=True, reason="test reason 1")], "test reason 1"),
            (
                [
                    mock.MagicMock(type="Complete", status=False),
                    mock.MagicMock(type="Failed", status=True, reason="test reason 2"),
                ],
                "test reason 2",
            ),
            (
                [
                    mock.MagicMock(type="Complete", status=True),
                    mock.MagicMock(type="Failed", status=True, reason="test reason 3"),
                ],
                "test reason 3",
            ),
        ],
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_failed(self, mock_merger, mock_loader, conditions, expected_result):
        mock_job = mock.MagicMock()
        mock_job.status.conditions = conditions

        hook = KubernetesHook()
        actual_result = hook.is_job_failed(mock_job)

        assert actual_result == expected_result

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_failed_no_status(self, mock_merger, mock_loader):
        mock_job = mock.MagicMock()
        mock_job.status = None

        hook = KubernetesHook()
        job_failed = hook.is_job_failed(mock_job)

        assert not job_failed

    @pytest.mark.parametrize(
        ("condition_type", "status", "expected_result"),
        [
            ("Complete", False, False),
            ("Complete", True, True),
            ("Failed", False, False),
            ("Failed", True, False),
            ("Suspended", False, False),
            ("Suspended", True, False),
            ("Unknown", False, False),
            ("Unknown", True, False),
        ],
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_successful(self, mock_merger, mock_loader, condition_type, status, expected_result):
        mock_job = mock.MagicMock()
        mock_job.status.conditions = [mock.MagicMock(type=condition_type, status=status)]

        hook = KubernetesHook()
        actual_result = hook.is_job_successful(mock_job)

        assert actual_result == expected_result

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_successful_no_status(self, mock_merger, mock_loader):
        mock_job = mock.MagicMock()
        mock_job.status = None

        hook = KubernetesHook()
        job_successful = hook.is_job_successful(mock_job)

        assert not job_successful

    @pytest.mark.parametrize(
        ("condition_type", "status", "expected_result"),
        [
            ("Complete", False, False),
            ("Complete", True, True),
            ("Failed", False, False),
            ("Failed", True, True),
            ("Suspended", False, False),
            ("Suspended", True, False),
            ("Unknown", False, False),
            ("Unknown", True, False),
        ],
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_complete(self, mock_merger, mock_loader, condition_type, status, expected_result):
        mock_job = mock.MagicMock()
        mock_job.status.conditions = [mock.MagicMock(type=condition_type, status=status)]

        hook = KubernetesHook()
        actual_result = hook.is_job_complete(mock_job)

        assert actual_result == expected_result

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_is_job_complete_no_status(self, mock_merger, mock_loader):
        mock_job = mock.MagicMock()
        mock_job.status = None

        hook = KubernetesHook()
        job_complete = hook.is_job_complete(mock_job)

        assert not job_complete

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch(f"{HOOK_MODULE}.KubernetesHook.get_job_status")
    def test_wait_until_job_complete(self, mock_job_status, mock_kube_config_merger, mock_kube_config_loader):
        job_expected = mock.MagicMock(
            status=mock.MagicMock(
                conditions=[
                    mock.MagicMock(type="TestType1"),
                    mock.MagicMock(type="TestType2"),
                    mock.MagicMock(type="Complete", status=True),
                ]
            )
        )
        mock_job_status.side_effect = [
            mock.MagicMock(status=mock.MagicMock(conditions=None)),
            mock.MagicMock(status=mock.MagicMock(conditions=[mock.MagicMock(type="TestType")])),
            mock.MagicMock(
                status=mock.MagicMock(
                    conditions=[
                        mock.MagicMock(type="TestType1"),
                        mock.MagicMock(type="TestType2"),
                    ]
                )
            ),
            mock.MagicMock(
                status=mock.MagicMock(
                    conditions=[
                        mock.MagicMock(type="TestType1"),
                        mock.MagicMock(type="TestType2"),
                        mock.MagicMock(type="Complete", status=False),
                    ]
                )
            ),
            job_expected,
        ]

        hook = KubernetesHook()
        with patch(f"{HOOK_MODULE}.sleep", return_value=None) as mock_sleep:
            job_actual = hook.wait_until_job_complete(
                job_name=JOB_NAME, namespace=NAMESPACE, job_poll_interval=POLL_INTERVAL
            )

        mock_job_status.assert_has_calls([mock.call(job_name=JOB_NAME, namespace=NAMESPACE)] * 5)
        mock_sleep.assert_has_calls([mock.call(POLL_INTERVAL)] * 4)
        assert job_actual == job_expected

    @patch(f"{HOOK_MODULE}.json.dumps")
    @patch(f"{HOOK_MODULE}.KubernetesHook.batch_v1_client")
    def test_create_job_retries_on_500_error(self, mock_client, mock_json_dumps):
        mock_client.create_namespaced_job.side_effect = [
            ApiException(status=500),
            MagicMock(),
        ]

        hook = KubernetesHook()
        hook.create_job(job=mock.MagicMock())

        assert mock_client.create_namespaced_job.call_count == 2

    @patch(f"{HOOK_MODULE}.json.dumps")
    @patch(f"{HOOK_MODULE}.KubernetesHook.batch_v1_client")
    def test_create_job_fails_on_other_exception(self, mock_client, mock_json_dumps):
        mock_client.create_namespaced_job.side_effect = [ApiException(status=404)]

        hook = KubernetesHook()
        with pytest.raises(ApiException):
            hook.create_job(job=mock.MagicMock())

    @patch(f"{HOOK_MODULE}.json.dumps")
    @patch(f"{HOOK_MODULE}.KubernetesHook.batch_v1_client")
    def test_create_job_retries_five_times(self, mock_client, mock_json_dumps):
        mock_client.create_namespaced_job.side_effect = [
            ApiException(status=500),
            ApiException(status=500),
            ApiException(status=500),
            ApiException(status=500),
            ApiException(status=500),
        ]

        hook = KubernetesHook()
        with pytest.raises(ApiException):
            hook.create_job(job=mock.MagicMock())

        assert mock_client.create_namespaced_job.call_count == 5

    @pytest.mark.parametrize(
        ("given_namespace", "expected_namespace"),
        [
            (None, "default-namespace"),
            ("given-namespace", "given-namespace"),
        ],
    )
    @pytest.mark.parametrize(
        ("given_client", "expected_client"),
        [
            (None, mock.MagicMock()),
            (mock_client := mock.MagicMock(), mock_client),  # type: ignore[name-defined]
        ],
    )
    @patch(f"{HOOK_MODULE}.utils.create_from_yaml")
    @patch(f"{HOOK_MODULE}.KubernetesHook.get_namespace")
    @patch(f"{HOOK_MODULE}.KubernetesHook.api_client", new_callable=PropertyMock)
    def test_apply_from_yaml_file(
        self,
        mock_api_client,
        mock_get_namespace,
        mock_create_from_yaml,
        given_client,
        expected_client,
        given_namespace,
        expected_namespace,
    ):
        initial_kwargs = dict(
            api_client=given_client,
            yaml_objects=mock.MagicMock(),
            yaml_file=mock.MagicMock(),
            verbose=mock.MagicMock(),
            namespace=given_namespace,
        )
        expected_kwargs = dict(
            k8s_client=expected_client,
            yaml_objects=initial_kwargs["yaml_objects"],
            yaml_file=initial_kwargs["yaml_file"],
            verbose=initial_kwargs["verbose"],
            namespace=expected_namespace,
        )
        mock_api_client.return_value = expected_client
        mock_get_namespace.return_value = expected_namespace

        KubernetesHook().apply_from_yaml_file(**initial_kwargs)

        mock_create_from_yaml.assert_called_once_with(**expected_kwargs)
        if given_client is None:
            mock_api_client.assert_called_once()
        if given_namespace is None:
            mock_get_namespace.assert_called_once()

    @mock.patch(HOOK_MODULE + ".sleep")
    @mock.patch(HOOK_MODULE + ".KubernetesHook.log")
    @mock.patch(HOOK_MODULE + ".KubernetesHook.get_deployment_status")
    def test_check_kueue_deployment_running(self, mock_get_deployment_status, mock_log, mock_sleep):
        mock_get_deployment_status.side_effect = [
            NOT_READY_DEPLOYMENT,
            READY_DEPLOYMENT,
        ]

        KubernetesHook().check_kueue_deployment_running(name=DEPLOYMENT_NAME, namespace=NAMESPACE)

        mock_log.info.assert_called_once_with("Waiting until Deployment will be ready...")
        mock_sleep.assert_called_once_with(2.0)

    @mock.patch(HOOK_MODULE + ".KubernetesHook.log")
    @mock.patch(HOOK_MODULE + ".KubernetesHook.get_deployment_status")
    def test_check_kueue_deployment_raise_exception(self, mock_get_deployment_status, mock_log):
        mock_get_deployment_status.side_effect = ValueError

        with pytest.raises(ValueError, match="Exception occurred while checking for Deployment status"):
            KubernetesHook().check_kueue_deployment_running(name=DEPLOYMENT_NAME, namespace=NAMESPACE)

        mock_log.exception.assert_called_once_with("Exception occurred while checking for Deployment status.")

    @mock.patch(f"{HOOK_MODULE}.yaml")
    @mock.patch(f"{HOOK_MODULE}.requests")
    def test_get_yaml_content_from_file(self, mock_requests, mock_yaml):
        mock_get = mock_requests.get
        mock_response = mock_get.return_value
        expected_response_text = "test response text"
        mock_response.text = expected_response_text
        mock_response.status_code = 200
        expected_result = list(mock_yaml.safe_load_all.return_value)

        result = KubernetesHook().get_yaml_content_from_file(YAML_URL)

        mock_get.assert_called_with(YAML_URL, allow_redirects=True)
        mock_yaml.safe_load_all.assert_called_with(expected_response_text)
        assert result == expected_result

    @mock.patch(f"{HOOK_MODULE}.yaml")
    @mock.patch(f"{HOOK_MODULE}.requests")
    def test_get_yaml_content_from_file_error(self, mock_requests, mock_yaml):
        mock_get = mock_requests.get
        mock_response = mock_get.return_value
        mock_response.status_code = 500
        expected_error_message = "Was not able to read the yaml file from given URL"

        with pytest.raises(AirflowException, match=expected_error_message):
            KubernetesHook().get_yaml_content_from_file(YAML_URL)

        mock_get.assert_called_with(YAML_URL, allow_redirects=True)
        mock_yaml.safe_load_all.assert_not_called()

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_load_config_with_config_dict(self, kube_config_merger, incluster_config, kube_config_loader):
        hook = KubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_dict={"a": "b"},
            cluster_context=None,
        )
        api_conn = hook.get_conn()
        assert not incluster_config.called
        assert hook._is_in_cluster is False
        kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)


class TestKubernetesHookIncorrectConfiguration:
    @pytest.mark.parametrize(
        "conn_uri",
        (
            "kubernetes://?kube_config_path=/tmp/&kube_config=[1,2,3]",
            "kubernetes://?kube_config_path=/tmp/&in_cluster=[1,2,3]",
            "kubernetes://?kube_config=/tmp/&in_cluster=[1,2,3]",
        ),
    )
    def test_should_raise_exception_on_invalid_configuration(self, conn_uri):
        kubernetes_hook = KubernetesHook()
        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_KUBERNETES_DEFAULT=conn_uri),
            pytest.raises(AirflowException, match="Invalid connection configuration"),
        ):
            kubernetes_hook.get_conn()


class TestAsyncKubernetesHook:
    KUBE_CONFIG_MERGER = "kubernetes_asyncio.config.kube_config.KubeConfigMerger"
    INCLUSTER_CONFIG_LOADER = "kubernetes_asyncio.config.incluster_config.InClusterConfigLoader"
    KUBE_LOADER_CONFIG = "kubernetes_asyncio.config.kube_config.KubeConfigLoader"
    KUBE_CONFIG_FROM_DICT = "kubernetes_asyncio.config.kube_config.load_kube_config_from_dict"
    KUBE_API = "kubernetes_asyncio.client.api.core_v1_api.CoreV1Api.{}"
    KUBE_BATCH_API = "kubernetes_asyncio.client.api.batch_v1_api.BatchV1Api.{}"
    KUBE_ASYNC_HOOK = HOOK_MODULE + ".AsyncKubernetesHook.{}"

    @staticmethod
    def mock_await_result(return_value):
        f = Future()
        f.set_result(return_value)
        return f

    @pytest_asyncio.fixture
    async def kube_config_loader(self):
        with mock.patch(self.KUBE_LOADER_CONFIG) as kube_config_loader:
            kube_config_loader.return_value.load_and_set.return_value = self.mock_await_result(None)
            yield kube_config_loader

    @staticmethod
    @pytest.fixture
    def kubernetes_connection(create_connection_without_db):
        extra = {"kube_config": '{"test": "kube"}'}
        create_connection_without_db(
            Connection(
                conn_type="kubernetes",
                conn_id=CONN_ID,
                extra=json.dumps(extra),
            ),
        )
        yield
        clear_test_connections()

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_LOADER_CONFIG)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_incluster(self, kube_config_merger, kube_config_loader, incluster_config):
        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=True,
            config_file=None,
            cluster_context=None,
        )
        await hook._load_config()
        incluster_config.assert_called_once()
        assert not kube_config_loader.called
        assert not kube_config_merger.called

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_config_dict(
        self, kube_config_merger, incluster_config, kube_config_loader
    ):
        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_dict={"a": "b"},
            cluster_context=None,
        )
        await hook._load_config()
        assert not incluster_config.called
        assert hook._is_in_cluster is False
        kube_config_loader.assert_called_once_with(
            config_dict={"a": "b"}, config_base_path=None, active_context=None, temp_file_path=None
        )

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_config_dict_and_cluster_context(
        self, kube_config_merger, incluster_config, kube_config_loader
    ):
        cluster_context = "some_kubernetes_cluster"
        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_dict={"a": "b"},
            cluster_context=cluster_context,
        )
        await hook._load_config()
        assert not incluster_config.called
        assert hook._is_in_cluster is False
        kube_config_loader.assert_called_once_with(
            config_dict={"a": "b"}, config_base_path=None, active_context=cluster_context, temp_file_path=None
        )

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_conn_id(
        self,
        kube_config_merger,
        incluster_config,
        kube_config_loader,
        kubernetes_connection,
    ):
        hook = AsyncKubernetesHook(
            conn_id=CONN_ID,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )
        await hook._load_config()
        assert not incluster_config.called
        kube_config_loader.assert_called_once()
        kube_config_merger.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_conn_id_kube_config_path(
        self, kube_config_merger, incluster_config, kube_config_loader, tmp_path, create_connection_without_db
    ):
        file_name = f"{tmp_path}/config"
        extra = {"kube_config_path": file_name}
        try:
            create_connection_without_db(
                Connection(
                    conn_type="kubernetes",
                    conn_id=CONN_ID,
                    extra=json.dumps(extra),
                ),
            )
            async with await anyio.open_file(file_name, "w+") as f:
                yaml.dump({"a": "b"}, f)
            hook = AsyncKubernetesHook(
                conn_id=CONN_ID,
                in_cluster=False,
                config_file=None,
                cluster_context=None,
            )
            await hook._load_config()
            assert not incluster_config.called
            kube_config_loader.assert_called_once()
            kube_config_merger.assert_called_once()
        except:
            raise
        finally:
            clear_test_connections()

    @pytest.mark.asyncio
    @mock.patch(INCLUSTER_CONFIG_LOADER)
    @mock.patch(KUBE_CONFIG_MERGER)
    async def test_load_config_with_default_client(
        self,
        kube_config_merger,
        incluster_config,
        kube_config_loader,
    ):
        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )
        kube_client = await hook._load_config()

        assert not incluster_config.called
        kube_config_loader.assert_called_once()
        kube_config_merger.assert_called_once()
        # It should return None in case when default client is used
        assert kube_client is None

    @pytest.mark.asyncio
    async def test_load_config_with_several_params(self, sdk_connection_not_found):
        hook = AsyncKubernetesHook(
            conn_id=CONN_ID,
            in_cluster=True,
            config_file=ASYNC_CONFIG_PATH,
            cluster_context=None,
        )
        with pytest.raises(AirflowException):
            await hook._load_config()

    @pytest.mark.asyncio
    @mock.patch(KUBE_API.format("read_namespaced_pod"))
    async def test_get_pod(self, lib_method, kube_config_loader):
        lib_method.return_value = self.mock_await_result(None)

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )
        await hook.get_pod(
            name=POD_NAME,
            namespace=NAMESPACE,
        )

        lib_method.assert_called_once()
        lib_method.assert_called_with(
            name=POD_NAME,
            namespace=NAMESPACE,
        )

    @pytest.mark.asyncio
    @mock.patch(KUBE_API.format("delete_namespaced_pod"))
    async def test_delete_pod(self, lib_method, kube_config_loader):
        lib_method.return_value = self.mock_await_result(None)

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )
        await hook.delete_pod(
            name=POD_NAME,
            namespace=NAMESPACE,
        )

        lib_method.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch(KUBE_API.format("read_namespaced_pod_log"))
    async def test_read_logs(self, lib_method, kube_config_loader):
        lib_method.return_value = self.mock_await_result("2023-01-11 Some string logs...")
        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )

        logs = await hook.read_logs(
            name=POD_NAME,
            namespace=NAMESPACE,
            container_name=CONTAINER_NAME,
            since_seconds=10,
        )

        lib_method.assert_called_once()
        lib_method.assert_called_with(
            name=POD_NAME,
            namespace=NAMESPACE,
            container=CONTAINER_NAME,
            follow=False,
            timestamps=True,
            since_seconds=10,
        )
        assert len(logs) == 1
        assert "2023-01-11 Some string logs..." in logs

    @pytest.mark.asyncio
    @mock.patch(KUBE_BATCH_API.format("read_namespaced_job_status"))
    async def test_get_job_status(self, lib_method, kube_config_loader):
        lib_method.return_value = self.mock_await_result(None)

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )
        await hook.get_job_status(
            name=JOB_NAME,
            namespace=NAMESPACE,
        )

        lib_method.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch(HOOK_MODULE + ".asyncio.sleep")
    @mock.patch(KUBE_ASYNC_HOOK.format("is_job_complete"))
    @mock.patch(KUBE_ASYNC_HOOK.format("get_job_status"))
    async def test_wait_until_job_complete(
        self, mock_get_job_status, mock_is_job_complete, mock_sleep, kube_config_loader
    ):
        mock_job_0, mock_job_1 = mock.MagicMock(), mock.MagicMock()
        mock_get_job_status.side_effect = mock.AsyncMock(side_effect=[mock_job_0, mock_job_1])
        mock_is_job_complete.side_effect = [False, True]

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )

        job_actual = await hook.wait_until_job_complete(
            name=JOB_NAME,
            namespace=NAMESPACE,
            poll_interval=10,
        )

        mock_get_job_status.assert_has_awaits(
            [
                mock.call(name=JOB_NAME, namespace=NAMESPACE),
                mock.call(name=JOB_NAME, namespace=NAMESPACE),
            ]
        )
        mock_is_job_complete.assert_has_calls([mock.call(job=mock_job_0), mock.call(job=mock_job_1)])
        mock_sleep.assert_awaited_once_with(10)
        assert job_actual == mock_job_1

    @pytest.mark.asyncio
    @mock.patch(HOOK_MODULE + ".asyncio.sleep")
    @mock.patch(HOOK_MODULE + ".container_is_completed")
    @mock.patch(KUBE_ASYNC_HOOK.format("get_pod"))
    async def test_wait_until_container_complete(
        self, mock_get_pod, mock_container_is_completed, mock_sleep, kube_config_loader
    ):
        mock_pod_0, mock_pod_1 = mock.MagicMock(), mock.MagicMock()
        mock_get_pod.side_effect = mock.AsyncMock(side_effect=[mock_pod_0, mock_pod_1])
        mock_container_is_completed.side_effect = [False, True]

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )

        await hook.wait_until_container_complete(
            name=POD_NAME,
            namespace=NAMESPACE,
            container_name=CONTAINER_NAME,
            poll_interval=10,
        )

        mock_get_pod.assert_has_awaits(
            [
                mock.call(name=POD_NAME, namespace=NAMESPACE),
                mock.call(name=POD_NAME, namespace=NAMESPACE),
            ]
        )
        mock_container_is_completed.assert_has_calls(
            [
                mock.call(pod=mock_pod_0, container_name=CONTAINER_NAME),
                mock.call(pod=mock_pod_1, container_name=CONTAINER_NAME),
            ]
        )
        mock_sleep.assert_awaited_once_with(10)

    @pytest.mark.asyncio
    @mock.patch(HOOK_MODULE + ".asyncio.sleep")
    @mock.patch(HOOK_MODULE + ".container_is_running")
    @mock.patch(KUBE_ASYNC_HOOK.format("get_pod"))
    async def test_wait_until_container_started(
        self, mock_get_pod, mock_container_is_running, mock_sleep, kube_config_loader
    ):
        mock_pod_0, mock_pod_1 = mock.MagicMock(), mock.MagicMock()
        mock_get_pod.side_effect = mock.AsyncMock(side_effect=[mock_pod_0, mock_pod_1])
        mock_container_is_running.side_effect = [False, True]

        hook = AsyncKubernetesHook(
            conn_id=None,
            in_cluster=False,
            config_file=None,
            cluster_context=None,
        )

        await hook.wait_until_container_started(
            name=POD_NAME,
            namespace=NAMESPACE,
            container_name=CONTAINER_NAME,
            poll_interval=10,
        )

        mock_get_pod.assert_has_awaits(
            [
                mock.call(name=POD_NAME, namespace=NAMESPACE),
                mock.call(name=POD_NAME, namespace=NAMESPACE),
            ]
        )
        mock_container_is_running.assert_has_calls(
            [
                mock.call(pod=mock_pod_0, container_name=CONTAINER_NAME),
                mock.call(pod=mock_pod_1, container_name=CONTAINER_NAME),
            ]
        )
        mock_sleep.assert_awaited_once_with(10)
