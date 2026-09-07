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
"""Execute commands in existing Kubernetes pods."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.providers.cncf.kubernetes.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import BaseOperator
else:
    from airflow.models import BaseOperator

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api, V1Pod
    from kubernetes.stream.ws_client import WSClient

    from airflow.sdk import Context

__all__ = ["KubernetesPodExecOperator"]

_DEFAULT_MAX_XCOM_OUTPUT_SIZE = 49_344


def _extract_complete_lines(buffer: str, chunk: str) -> tuple[list[str], str]:
    """Extract newline-delimited lines while retaining the incomplete remainder."""
    *lines, remainder = f"{buffer}{chunk}".split("\n")
    return lines, remainder


class KubernetesPodExecOperator(BaseOperator):
    """
    Execute a command in a running container of an existing Kubernetes pod.

    The operator does not create, restart, or delete the target pod. Commands are executed directly,
    without a shell; include a shell explicitly in ``command`` when shell features are required.

    :param pod_name: Name of the existing Kubernetes pod. (templated)
    :param command: Command and arguments to execute in the container. (templated)
    :param namespace: Namespace containing the pod. Defaults to the namespace configured in the
        Kubernetes connection, then ``default``. (templated)
    :param container_name: Name of the container in which to execute the command. When omitted, the
        ``kubectl.kubernetes.io/default-container`` annotation or the first container is used. (templated)
    :param kubernetes_conn_id: The :ref:`Kubernetes connection <howto/connection:kubernetes>` to use.
        (templated)
    :param in_cluster: Use in-cluster Kubernetes configuration.
    :param cluster_context: Context to use from the kubeconfig. (templated)
    :param config_file: Path to the kubeconfig file. (templated)
    :param do_xcom_push: Return standard output through XCom when ``True``. Defaults to ``False``.
    :param max_xcom_output_size: Maximum UTF-8 byte size retained for XCom. Defaults to 49,344 bytes.
    """

    template_fields: Sequence[str] = (
        "pod_name",
        "command",
        "namespace",
        "container_name",
        "kubernetes_conn_id",
        "cluster_context",
        "config_file",
    )
    template_fields_renderers = {"command": "py"}

    def __init__(
        self,
        *,
        pod_name: str,
        command: Sequence[str],
        namespace: str | None = None,
        container_name: str | None = None,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        do_xcom_push: bool = False,
        max_xcom_output_size: int = _DEFAULT_MAX_XCOM_OUTPUT_SIZE,
        **kwargs,
    ) -> None:
        super().__init__(do_xcom_push=do_xcom_push, **kwargs)
        if (
            isinstance(max_xcom_output_size, bool)
            or not isinstance(max_xcom_output_size, int)
            or max_xcom_output_size <= 0
        ):
            raise ValueError("`max_xcom_output_size` must be a positive integer")
        self.pod_name = pod_name
        self.command = command
        self.namespace = namespace
        self.container_name = container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.max_xcom_output_size = max_xcom_output_size
        self._exec_client: WSClient | None = None
        self._exec_target: tuple[str, str, str] | None = None

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    def _resolve_namespace(self) -> str:
        return self.namespace or self.hook.get_namespace() or KubernetesHook.DEFAULT_NAMESPACE

    def _validate_command(self) -> list[str]:
        if isinstance(self.command, str) or not isinstance(self.command, Sequence):
            raise TypeError("`command` must be a sequence of strings, not a single string")
        if not self.command:
            raise ValueError("`command` must contain at least one element")
        if not all(isinstance(argument, str) for argument in self.command):
            raise TypeError("Every element of `command` must be a string")
        return list(self.command)

    def _resolve_container_name(self, pod: V1Pod) -> str:
        containers = pod.spec.containers if pod.spec and pod.spec.containers else []
        container_names = [container.name for container in containers]
        if not container_names:
            raise RuntimeError(f"Pod {self.pod_name!r} does not define any containers")

        if self.container_name:
            if self.container_name not in container_names:
                raise ValueError(f"Container {self.container_name!r} does not exist in pod {self.pod_name!r}")
            return self.container_name

        annotations = pod.metadata.annotations if pod.metadata and pod.metadata.annotations else {}
        default_container = annotations.get("kubectl.kubernetes.io/default-container")
        if isinstance(default_container, str) and default_container in container_names:
            return default_container
        return container_names[0]

    def _validate_container_is_running(self, pod: V1Pod, container_name: str) -> None:
        if not pod.status or pod.status.phase != PodPhase.RUNNING:
            phase = pod.status.phase if pod.status else None
            raise RuntimeError(
                f"Cannot execute a command in pod {self.pod_name!r} while it is in phase {phase!r}"
            )

        statuses = pod.status.container_statuses or []
        container_status = next((status for status in statuses if status.name == container_name), None)
        if (
            container_status is None
            or container_status.state is None
            or container_status.state.running is None
        ):
            raise RuntimeError(
                f"Container {container_name!r} in pod {self.pod_name!r} is not running or "
                "container status cannot be retrieved"
            )

    def _log_lines(self, lines: Sequence[str], *, stream_name: str) -> None:
        for line in lines:
            self.log.info("[%s] %s", stream_name, line.removesuffix("\r"))

    def _consume_output(self, exec_client: WSClient) -> str:
        stdout_chunks: list[str] = []
        stdout_size = 0
        stdout_buffer = ""
        stderr_buffer = ""
        try:
            while exec_client.is_open():
                exec_client.update(timeout=1)
                while exec_client.peek_stdout():
                    output = exec_client.read_stdout()
                    stdout_lines, stdout_buffer = _extract_complete_lines(stdout_buffer, output)
                    self._log_lines(stdout_lines, stream_name="stdout")
                    if self.do_xcom_push:
                        stdout_size += len(output.encode("utf-8"))
                        if stdout_size > self.max_xcom_output_size:
                            raise RuntimeError(
                                "Standard output exceeded the configured XCom limit of "
                                f"{self.max_xcom_output_size} bytes"
                            )
                        stdout_chunks.append(output)
                while exec_client.peek_stderr():
                    stderr_lines, stderr_buffer = _extract_complete_lines(
                        stderr_buffer, exec_client.read_stderr()
                    )
                    self._log_lines(stderr_lines, stream_name="stderr")
        finally:
            if stdout_buffer:
                self._log_lines((stdout_buffer,), stream_name="stdout")
            if stderr_buffer:
                self._log_lines((stderr_buffer,), stream_name="stderr")
        return "".join(stdout_chunks)

    def _close_exec_client(self) -> None:
        exec_client = self._exec_client
        exec_target = self._exec_target
        self._exec_client = None
        self._exec_target = None
        if exec_client is None:
            return
        try:
            exec_client.close()
        except Exception:
            namespace, pod_name, container_name = exec_target or (
                self.namespace,
                self.pod_name,
                self.container_name,
            )
            self.log.exception(
                "Failed to close Kubernetes exec connection for container %s in pod %s/%s",
                container_name,
                namespace,
                pod_name,
            )

    def execute(self, context: Context) -> str | None:
        command = self._validate_command()
        namespace = self._resolve_namespace()
        if not self.pod_name:
            raise ValueError("`pod_name` must not be empty")

        try:
            pod = self.hook.get_pod(name=self.pod_name, namespace=namespace)
        except ApiException as error:
            raise RuntimeError(
                f"Unable to read pod {namespace}/{self.pod_name}: {error.reason or error}"
            ) from error

        container_name = self._resolve_container_name(pod)
        self._validate_container_is_running(pod, container_name)
        self.log.info(
            "Executing command in container %s of pod %s/%s", container_name, namespace, self.pod_name
        )

        self._exec_target = (namespace, self.pod_name, container_name)
        try:
            exec_client = kubernetes_stream(
                self.client.connect_get_namespaced_pod_exec,
                name=self.pod_name,
                namespace=namespace,
                container=container_name,
                command=command,
                stdin=False,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
            self._exec_client = exec_client
            output = self._consume_output(exec_client)
            return_code = exec_client.returncode
        except ApiException as error:
            raise RuntimeError(
                f"Unable to execute command in pod {namespace}/{self.pod_name}: {error.reason or error}"
            ) from error
        finally:
            self._close_exec_client()

        if return_code is None:
            raise RuntimeError(
                f"Command in container {container_name!r} of pod {namespace}/{self.pod_name} ended "
                "without reporting an exit code"
            )
        if return_code != 0:
            raise RuntimeError(
                f"Command in container {container_name!r} of pod {namespace}/{self.pod_name} "
                f"failed with exit code {return_code}"
            )
        return output if self.do_xcom_push else None

    def on_kill(self) -> None:
        """Close the active Kubernetes exec connection without modifying the target pod."""
        self._close_exec_client()
