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

from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException
from kubernetes.stream.ws_client import WSClient

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase

MODULE = "airflow.providers.cncf.kubernetes.operators.pod_exec"


def create_pod(
    *,
    container_names: tuple[str, ...] = ("main",),
    annotations: dict[str, str] | None = None,
    phase: str = PodPhase.RUNNING,
    container_statuses=None,
    with_spec: bool = True,
    with_status: bool = True,
):
    if container_statuses is None:
        container_statuses = [
            SimpleNamespace(name=name, state=SimpleNamespace(running=object())) for name in container_names
        ]
    spec = SimpleNamespace(containers=[SimpleNamespace(name=name) for name in container_names])
    status = SimpleNamespace(phase=phase, container_statuses=container_statuses)
    return SimpleNamespace(
        metadata=SimpleNamespace(annotations=annotations),
        spec=spec if with_spec else None,
        status=status if with_status else None,
    )


def create_exec_client(*, stdout=(), stderr=(), returncode=0):
    exec_client = mock.MagicMock(spec=WSClient)
    exec_client.is_open.side_effect = [True, False]
    exec_client.peek_stdout.side_effect = [*stdout, ""]
    exec_client.read_stdout.side_effect = stdout
    exec_client.peek_stderr.side_effect = [*stderr, ""]
    exec_client.read_stderr.side_effect = stderr
    exec_client.returncode = returncode
    return exec_client


def create_operator(*, pod=None, hook_namespace=None, **kwargs):
    operator = KubernetesPodExecOperator(
        task_id="exec",
        pod_name="existing-pod",
        command=["echo", "hello"],
        **kwargs,
    )
    hook = mock.MagicMock(spec=KubernetesHook)
    hook.get_namespace.return_value = hook_namespace
    hook.get_pod.return_value = pod or create_pod()
    operator.__dict__["hook"] = hook
    return operator, hook


class TestKubernetesPodExecOperator:
    def test_template_fields(self):
        assert set(KubernetesPodExecOperator.template_fields) == {
            "pod_name",
            "command",
            "namespace",
            "container_name",
            "kubernetes_conn_id",
            "cluster_context",
            "config_file",
        }

    @mock.patch(f"{MODULE}.KubernetesHook", autospec=True)
    def test_hook_configuration(self, kubernetes_hook_mock):
        operator = KubernetesPodExecOperator(
            task_id="exec",
            pod_name="existing-pod",
            command=["date"],
            kubernetes_conn_id="kubernetes_test",
            in_cluster=True,
            config_file="/tmp/kubeconfig",
            cluster_context="test-context",
        )

        assert operator.hook is kubernetes_hook_mock.return_value
        kubernetes_hook_mock.assert_called_once_with(
            conn_id="kubernetes_test",
            in_cluster=True,
            config_file="/tmp/kubeconfig",
            cluster_context="test-context",
        )

    @pytest.mark.parametrize(
        ("do_xcom_push", "expected_result"),
        [(False, None), (True, "hello\nworld\n")],
    )
    @mock.patch(f"{MODULE}.KubernetesPodExecOperator.log", spec=["info", "warning"])
    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_execute(self, kubernetes_stream_mock, log_mock, do_xcom_push, expected_result):
        operator, hook = create_operator(
            namespace="test-namespace",
            container_name="main",
            do_xcom_push=do_xcom_push,
        )
        exec_client = create_exec_client(
            stdout=("hello", "\nworld", "\n"), stderr=("warning", "\n"), returncode=0
        )
        kubernetes_stream_mock.return_value = exec_client

        result = operator.execute(context={})

        assert result == expected_result
        hook.get_pod.assert_called_once_with(name="existing-pod", namespace="test-namespace")
        kubernetes_stream_mock.assert_called_once_with(
            hook.core_v1_client.connect_get_namespaced_pod_exec,
            name="existing-pod",
            namespace="test-namespace",
            container="main",
            command=["echo", "hello"],
            stdin=False,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )
        exec_client.update.assert_called_once_with(timeout=1)
        assert exec_client.read_stdout.call_count == 3
        assert exec_client.read_stderr.call_count == 2
        exec_client.close.assert_called_once_with()
        assert operator._exec_client is None
        assert operator._exec_target is None
        log_mock.info.assert_has_calls(
            [
                mock.call("[%s] %s", "stdout", "hello"),
                mock.call("[%s] %s", "stdout", "world"),
                mock.call("[%s] %s", "stderr", "warning"),
            ]
        )
        log_mock.warning.assert_not_called()

    @pytest.mark.parametrize("max_xcom_output_size", [0, -1, True, 1.5])
    def test_rejects_invalid_max_xcom_output_size(self, max_xcom_output_size):
        with pytest.raises(ValueError, match="must be a positive integer"):
            create_operator(max_xcom_output_size=max_xcom_output_size)

    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_rejects_xcom_output_over_limit(self, kubernetes_stream_mock):
        operator, _ = create_operator(namespace="test-namespace", do_xcom_push=True, max_xcom_output_size=3)
        exec_client = create_exec_client(stdout=("é", "é"))
        kubernetes_stream_mock.return_value = exec_client

        with pytest.raises(RuntimeError, match="XCom limit of 3 bytes"):
            operator.execute(context={})

        exec_client.close.assert_called_once_with()

    @mock.patch(f"{MODULE}.KubernetesPodExecOperator.log", spec=["info"])
    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_execute_flushes_incomplete_log_lines(self, kubernetes_stream_mock, log_mock):
        operator, _ = create_operator(namespace="test-namespace", container_name="main")
        kubernetes_stream_mock.return_value = create_exec_client(
            stdout=("partial ", "stdout"), stderr=("partial ", "stderr")
        )

        operator.execute(context={})

        log_mock.info.assert_has_calls(
            [
                mock.call("[%s] %s", "stdout", "partial stdout"),
                mock.call("[%s] %s", "stderr", "partial stderr"),
            ]
        )

    @pytest.mark.parametrize(
        ("namespace", "hook_namespace", "expected_namespace"),
        [
            ("task-namespace", "connection-namespace", "task-namespace"),
            (None, "connection-namespace", "connection-namespace"),
            (None, None, KubernetesHook.DEFAULT_NAMESPACE),
        ],
    )
    def test_resolve_namespace(self, namespace, hook_namespace, expected_namespace):
        operator, _ = create_operator(namespace=namespace, hook_namespace=hook_namespace)

        assert operator._resolve_namespace() == expected_namespace

    @pytest.mark.parametrize(
        ("container_name", "annotations", "expected_container"),
        [
            ("secondary", {"kubectl.kubernetes.io/default-container": "main"}, "secondary"),
            (None, {"kubectl.kubernetes.io/default-container": "secondary"}, "secondary"),
            (None, {"kubectl.kubernetes.io/default-container": "missing"}, "main"),
            (None, None, "main"),
        ],
    )
    def test_resolve_container_name(self, container_name, annotations, expected_container):
        pod = create_pod(container_names=("main", "secondary"), annotations=annotations)
        operator, _ = create_operator(pod=pod, container_name=container_name)

        assert operator._resolve_container_name(pod) == expected_container

    @pytest.mark.parametrize("with_spec", [False, True])
    def test_rejects_pod_without_containers(self, with_spec):
        pod = create_pod(container_names=(), with_spec=with_spec)
        operator, _ = create_operator(pod=pod)

        with pytest.raises(RuntimeError, match="does not define any containers"):
            operator._resolve_container_name(pod)

    def test_rejects_unknown_container(self):
        pod = create_pod()
        operator, _ = create_operator(pod=pod, container_name="missing")

        with pytest.raises(ValueError, match="does not exist"):
            operator._resolve_container_name(pod)

    @pytest.mark.parametrize(
        ("pod", "expected_message"),
        [
            (create_pod(with_status=False), "phase None"),
            (create_pod(phase="Pending"), "phase 'Pending'"),
            (create_pod(container_statuses=[]), "is not running or container status cannot be retrieved"),
            (
                create_pod(container_statuses=[SimpleNamespace(name="main", state=None)]),
                "is not running or container status cannot be retrieved",
            ),
            (
                create_pod(
                    container_statuses=[SimpleNamespace(name="main", state=SimpleNamespace(running=None))]
                ),
                "is not running or container status cannot be retrieved",
            ),
        ],
    )
    def test_rejects_unavailable_target(self, pod, expected_message):
        operator, _ = create_operator(pod=pod)

        with pytest.raises(RuntimeError, match=expected_message):
            operator._validate_container_is_running(pod, "main")

    @pytest.mark.parametrize(
        ("command", "error", "expected_message"),
        [
            ("echo hello", TypeError, "sequence of strings"),
            (42, TypeError, "sequence of strings"),
            ([], ValueError, "at least one element"),
            (["echo", 42], TypeError, "Every element"),
        ],
    )
    def test_rejects_invalid_command(self, command, error, expected_message):
        operator, _ = create_operator()
        operator.command = command

        with pytest.raises(error, match=expected_message):
            operator.execute(context={})

    def test_rejects_empty_pod_name(self):
        operator, _ = create_operator()
        operator.pod_name = ""

        with pytest.raises(ValueError, match="must not be empty"):
            operator.execute(context={})

    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_wraps_pod_read_error(self, kubernetes_stream_mock):
        operator, hook = create_operator(namespace="test-namespace")
        hook.get_pod.side_effect = ApiException(status=404, reason="Not Found")

        with pytest.raises(RuntimeError, match="Unable to read pod.*Not Found"):
            operator.execute(context={})

        kubernetes_stream_mock.assert_not_called()

    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_wraps_exec_api_error(self, kubernetes_stream_mock):
        operator, _ = create_operator(namespace="test-namespace")
        kubernetes_stream_mock.side_effect = ApiException(status=403, reason="Forbidden")

        with pytest.raises(RuntimeError, match="Unable to execute command.*Forbidden"):
            operator.execute(context={})

        assert operator._exec_client is None

    @pytest.mark.parametrize(
        ("returncode", "expected_message"),
        [(None, "without reporting an exit code"), (17, "failed with exit code 17")],
    )
    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_rejects_unsuccessful_command(self, kubernetes_stream_mock, returncode, expected_message):
        operator, _ = create_operator(namespace="test-namespace")
        exec_client = create_exec_client(returncode=returncode)
        kubernetes_stream_mock.return_value = exec_client

        with pytest.raises(RuntimeError, match=expected_message):
            operator.execute(context={})

        exec_client.close.assert_called_once_with()

    def test_on_kill_closes_active_connection(self):
        operator, _ = create_operator()
        exec_client = mock.MagicMock(spec=WSClient)
        operator._exec_client = exec_client

        operator.on_kill()

        exec_client.close.assert_called_once_with()
        assert operator._exec_client is None
        assert operator._exec_target is None

    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_on_kill_while_consuming_output(self, kubernetes_stream_mock):
        operator, _ = create_operator(namespace="test-namespace")
        exec_client = create_exec_client()
        exec_client.is_open.side_effect = lambda: (operator.on_kill(), False)[1]
        kubernetes_stream_mock.return_value = exec_client

        assert operator.execute(context={}) is None

        exec_client.close.assert_called_once_with()
        assert operator._exec_client is None
        assert operator._exec_target is None

    @mock.patch(f"{MODULE}.kubernetes_stream", autospec=True)
    def test_execute_closes_connection_when_output_consumption_fails(self, kubernetes_stream_mock):
        operator, _ = create_operator(namespace="test-namespace")
        exec_client = create_exec_client()
        exec_client.update.side_effect = RuntimeError("WebSocket update failed")
        kubernetes_stream_mock.return_value = exec_client

        with pytest.raises(RuntimeError, match="WebSocket update failed"):
            operator.execute(context={})

        exec_client.close.assert_called_once_with()
        assert operator._exec_client is None
        assert operator._exec_target is None

    def test_on_kill_without_active_connection(self):
        operator, _ = create_operator()

        operator.on_kill()

        assert operator._exec_client is None
        assert operator._exec_target is None

    @mock.patch(f"{MODULE}.KubernetesPodExecOperator.log", spec=["exception"])
    def test_close_error_does_not_mask_task_shutdown(self, log_mock):
        operator, _ = create_operator()
        exec_client = mock.MagicMock(spec=WSClient)
        exec_client.close.side_effect = RuntimeError("connection already closed")
        operator._exec_client = exec_client
        operator._exec_target = ("test-namespace", "existing-pod", "main")

        operator.on_kill()

        assert operator._exec_client is None
        assert operator._exec_target is None
        log_mock.exception.assert_called_once_with(
            "Failed to close Kubernetes exec connection for container %s in pod %s/%s",
            "main",
            "test-namespace",
            "existing-pod",
        )
