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
"""Launches PODs."""

from __future__ import annotations

import asyncio
import enum
import json
import math
import time
from collections.abc import Generator, Iterable
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Protocol, cast

import pendulum
import tenacity
from kubernetes import client, watch
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError
from urllib3.exceptions import HTTPError, TimeoutError

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode, KubernetesPodOperatorCallback
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import utcnow

if TYPE_CHECKING:
    from kubernetes.client.models.core_v1_event_list import CoreV1EventList
    from kubernetes.client.models.v1_container_state import V1ContainerState
    from kubernetes.client.models.v1_container_state_waiting import V1ContainerStateWaiting
    from kubernetes.client.models.v1_container_status import V1ContainerStatus
    from kubernetes.client.models.v1_object_reference import V1ObjectReference
    from kubernetes.client.models.v1_pod import V1Pod
    from kubernetes.client.models.v1_pod_condition import V1PodCondition
    from urllib3.response import HTTPResponse


EMPTY_XCOM_RESULT = "__airflow_xcom_result_empty__"
"""
Sentinel for no xcom result.

:meta private:
"""


class PodLaunchFailedException(AirflowException):
    """When pod launching fails in KubernetesPodOperator."""


def should_retry_start_pod(exception: BaseException) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying."""
    if isinstance(exception, ApiException):
        return str(exception.status) == "409"
    return False


class PodPhase:
    """
    Possible pod phases.

    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"

    terminal_states = {FAILED, SUCCEEDED}


class PodOperatorHookProtocol(Protocol):
    """
    Protocol to define methods relied upon by KubernetesPodOperator.

    Subclasses of KubernetesPodOperator, such as GKEStartPodOperator, may use
    hooks that don't extend KubernetesHook.  We use this protocol to document the
    methods used by KPO and ensure that these methods exist on such other hooks.
    """

    @property
    def core_v1_client(self) -> client.CoreV1Api:
        """Get authenticated client object."""

    @property
    def is_in_cluster(self) -> bool:
        """Expose whether the hook is configured with ``load_incluster_config`` or not."""

    def get_pod(self, name: str, namespace: str) -> V1Pod:
        """Read pod object from kubernetes API."""

    def get_namespace(self) -> str | None:
        """Return the namespace that defined in the connection."""

    def get_xcom_sidecar_container_image(self) -> str | None:
        """Return the xcom sidecar image that defined in the connection."""

    def get_xcom_sidecar_container_resources(self) -> str | None:
        """Return the xcom sidecar resources that defined in the connection."""


def get_container_status(pod: V1Pod, container_name: str) -> V1ContainerStatus | None:
    """Retrieve container status."""
    if pod and pod.status:
        container_statuses = []
        if pod.status.container_statuses:
            container_statuses.extend(pod.status.container_statuses)
        if pod.status.init_container_statuses:
            container_statuses.extend(pod.status.init_container_statuses)

    else:
        container_statuses = None

    if container_statuses:
        # In general the variable container_statuses can store multiple items matching different containers.
        # The following generator expression yields all items that have name equal to the container_name.
        # The function next() here calls the generator to get only the first value. If there's nothing found
        # then None is returned.
        return next((x for x in container_statuses if x.name == container_name), None)
    return None


def container_is_running(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is running.

    If that container is present and running, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.running is not None


def container_is_completed(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed.

    If that container is present and completed, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.terminated is not None


def container_is_succeeded(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed and succeeded.

    If that container is present and completed and succeeded, returns True.  Returns False otherwise.
    """
    if not container_is_completed(pod, container_name):
        return False

    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.terminated.exit_code == 0


def container_is_wait(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is waiting.

    If that container is present and waiting, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False

    return container_status.state.waiting is not None


def container_is_terminated(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is terminated.

    If that container is present and terminated, returns True.  Returns False otherwise.
    """
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if not container_statuses:
        return False
    container_status = next((x for x in container_statuses if x.name == container_name), None)
    if not container_status:
        return False
    return container_status.state.terminated is not None


def get_container_termination_message(pod: V1Pod, container_name: str):
    with suppress(AttributeError, TypeError):
        container_statuses = pod.status.container_statuses
        container_status = next((x for x in container_statuses if x.name == container_name), None)
        return container_status.state.terminated.message if container_status else None


def check_exception_is_kubernetes_api_unauthorized(exc: BaseException):
    return isinstance(exc, ApiException) and exc.status and str(exc.status) == "401"


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class PodNotFoundException(AirflowException):
    """Expected pod does not exist in kube-api."""


class PodLogsConsumer:
    """
    Responsible for pulling pod logs from a stream with checking a container status before reading data.

    This class is a workaround for the issue https://github.com/apache/airflow/issues/23497.

    :param response: HTTP response with logs
    :param pod: Pod instance from Kubernetes client
    :param pod_manager: Pod manager instance
    :param container_name: Name of the container that we're reading logs from
    :param post_termination_timeout: (Optional) The period of time in seconds representing for how long time
        logs are available after the container termination.
    :param read_pod_cache_timeout: (Optional) The container's status cache lifetime.
        The container status is cached to reduce API calls.

    :meta private:
    """

    def __init__(
        self,
        response: HTTPResponse,
        pod: V1Pod,
        pod_manager: PodManager,
        container_name: str,
        post_termination_timeout: int = 120,
        read_pod_cache_timeout: int = 120,
    ):
        self.response = response
        self.pod = pod
        self.pod_manager = pod_manager
        self.container_name = container_name
        self.post_termination_timeout = post_termination_timeout
        self.last_read_pod_at = None
        self.read_pod_cache = None
        self.read_pod_cache_timeout = read_pod_cache_timeout

    def __iter__(self) -> Generator[bytes, None, None]:
        r"""Yield log items divided by the '\n' symbol."""
        incomplete_log_item: list[bytes] = []
        if self.logs_available():
            for data_chunk in self.response.stream(amt=None, decode_content=True):
                if b"\n" in data_chunk:
                    log_items = data_chunk.split(b"\n")
                    yield from self._extract_log_items(incomplete_log_item, log_items)
                    incomplete_log_item = self._save_incomplete_log_item(log_items[-1])
                else:
                    incomplete_log_item.append(data_chunk)
                if not self.logs_available():
                    break
        if incomplete_log_item:
            yield b"".join(incomplete_log_item)

    @staticmethod
    def _extract_log_items(incomplete_log_item: list[bytes], log_items: list[bytes]):
        yield b"".join(incomplete_log_item) + log_items[0] + b"\n"
        for x in log_items[1:-1]:
            yield x + b"\n"

    @staticmethod
    def _save_incomplete_log_item(sub_chunk: bytes):
        return [sub_chunk] if [sub_chunk] else []

    def logs_available(self):
        remote_pod = self.read_pod()
        if container_is_running(pod=remote_pod, container_name=self.container_name):
            return True
        container_status = get_container_status(pod=remote_pod, container_name=self.container_name)
        state = container_status.state if container_status else None
        terminated = state.terminated if state else None
        if terminated:
            termination_time = terminated.finished_at
            if termination_time:
                return termination_time + timedelta(seconds=self.post_termination_timeout) > utcnow()
        return False

    def read_pod(self):
        _now = utcnow()
        if (
            self.read_pod_cache is None
            or self.last_read_pod_at + timedelta(seconds=self.read_pod_cache_timeout) < _now
        ):
            self.read_pod_cache = self.pod_manager.read_pod(self.pod)
            self.last_read_pod_at = _now
        return self.read_pod_cache


@dataclass
class PodLoggingStatus:
    """Return the status of the pod and last log time when exiting from `fetch_container_logs`."""

    running: bool
    last_log_time: DateTime | None


class PodManager(LoggingMixin):
    """Create, monitor, and otherwise interact with Kubernetes pods for use with the KubernetesPodOperator."""

    def __init__(
        self,
        kube_client: client.CoreV1Api,
        callbacks: list[type[KubernetesPodOperatorCallback]] | None = None,
    ):
        """
        Create the launcher.

        :param kube_client: kubernetes client
        :param callbacks:
        """
        super().__init__()
        self._client = kube_client
        self._watch = watch.Watch()
        self._callbacks = callbacks or []
        self.stop_watching_events = False

    def run_pod_async(self, pod: V1Pod, **kwargs) -> V1Pod:
        """Run POD asynchronously."""
        sanitized_pod = self._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug("Pod Creation Request: \n%s", json_pod)
        try:
            resp = self._client.create_namespaced_pod(
                body=sanitized_pod, namespace=pod.metadata.namespace, **kwargs
            )
            self.log.debug("Pod Creation Response: %s", resp)
        except Exception as e:
            self.log.exception(
                "Exception when attempting to create Namespaced Pod: %s", str(json_pod).replace("\n", " ")
            )
            raise e
        return resp

    def delete_pod(self, pod: V1Pod) -> None:
        """Delete POD."""
        try:
            self._client.delete_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace, body=client.V1DeleteOptions()
            )
        except ApiException as e:
            # If the pod is already deleted
            if str(e.status) != "404":
                raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_pod),
    )
    def create_pod(self, pod: V1Pod) -> V1Pod:
        """Launch the pod asynchronously."""
        return self.run_pod_async(pod)

    async def watch_pod_events(self, pod: V1Pod, check_interval: int = 1) -> None:
        """Read pod events and writes into log."""
        num_events = 0
        while not self.stop_watching_events:
            events = self.read_pod_events(pod)
            for new_event in events.items[num_events:]:
                involved_object: V1ObjectReference = new_event.involved_object
                self.log.info(
                    "The Pod has an Event: %s from %s", new_event.message, involved_object.field_path
                )
            num_events = len(events.items)
            await asyncio.sleep(check_interval)

    async def await_pod_start(
        self, pod: V1Pod, schedule_timeout: int = 120, startup_timeout: int = 120, check_interval: int = 1
    ) -> None:
        """
        Wait for the pod to reach phase other than ``Pending``.

        :param pod:
        :param schedule_timeout: Timeout (in seconds) for pod stay in schedule state
            (if pod is taking to long in schedule state, fails task)
        :param startup_timeout: Timeout (in seconds) for startup of the pod
            (if pod is pending for too long after being scheduled, fails task)
        :param check_interval: Interval (in seconds) between checks
        :return:
        """
        self.log.info("::group::Waiting until %ss to get the POD scheduled...", schedule_timeout)
        pod_was_scheduled = False
        start_check_time = time.time()
        while True:
            remote_pod = self.read_pod(pod)
            pod_status = remote_pod.status
            if pod_status.phase != PodPhase.PENDING:
                self.stop_watching_events = True
                self.log.info("::endgroup::")
                break

            # Check for timeout
            pod_conditions: list[V1PodCondition] = pod_status.conditions
            if pod_conditions and any(
                (condition.type == "PodScheduled" and condition.status == "True")
                for condition in pod_conditions
            ):
                if not pod_was_scheduled:
                    # POD was initially scheduled update timeout for getting POD launched
                    pod_was_scheduled = True
                    start_check_time = time.time()
                    self.log.info("Waiting %ss to get the POD running...", startup_timeout)

                if time.time() - start_check_time >= startup_timeout:
                    self.log.info("::endgroup::")
                    raise PodLaunchFailedException(
                        f"Pod took too long to start. More than {startup_timeout}s. Check the pod events in kubernetes."
                    )
            else:
                if time.time() - start_check_time >= schedule_timeout:
                    self.log.info("::endgroup::")
                    raise PodLaunchFailedException(
                        f"Pod took too long to be scheduled on the cluster, giving up. More than {schedule_timeout}s. Check the pod events in kubernetes."
                    )

            # Check for general problems to terminate early - ErrImagePull
            if pod_status.container_statuses:
                for container_status in pod_status.container_statuses:
                    container_state: V1ContainerState = container_status.state
                    container_waiting: V1ContainerStateWaiting | None = container_state.waiting
                    if container_waiting:
                        if container_waiting.reason in ["ErrImagePull", "InvalidImageName"]:
                            self.log.info("::endgroup::")
                            raise PodLaunchFailedException(
                                f"Pod docker image cannot be pulled, unable to start: {container_waiting.reason}"
                                f"\n{container_waiting.message}"
                            )

            await asyncio.sleep(check_interval)

    def fetch_container_logs(
        self,
        pod: V1Pod,
        container_name: str,
        *,
        follow=False,
        since_time: DateTime | None = None,
        post_termination_timeout: int = 120,
    ) -> PodLoggingStatus:
        """
        Follow the logs of container and stream to airflow logging.

        Returns when container exits.

        Between when the pod starts and logs being available, there might be a delay due to CSR not approved
        and signed yet. In such situation, ApiException is thrown. This is why we are retrying on this
        specific exception.

        :meta private:
        """

        def consume_logs(*, since_time: DateTime | None = None) -> tuple[DateTime | None, Exception | None]:
            """
            Try to follow container logs until container completes.

            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """
            exception = None
            last_captured_timestamp = None
            # We timeout connections after 30 minutes because otherwise they can get
            # stuck forever. The 30 is somewhat arbitrary.
            # As a consequence, a TimeoutError will be raised no more than 30 minutes
            # after starting read.
            connection_timeout = 60 * 30
            # We set a shorter read timeout because that helps reduce *connection* timeouts
            # (since the connection will be restarted periodically). And with read timeout,
            # we don't need to worry about either duplicate messages or losing messages; we
            # can safely resume from a few seconds later
            read_timeout = 60 * 5
            try:
                logs = self.read_pod_logs(
                    pod=pod,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=(
                        math.ceil((pendulum.now() - since_time).total_seconds()) if since_time else None
                    ),
                    follow=follow,
                    post_termination_timeout=post_termination_timeout,
                    _request_timeout=(connection_timeout, read_timeout),
                )
                message_to_log = None
                message_timestamp = None
                progress_callback_lines = []
                try:
                    for raw_line in logs:
                        line = raw_line.decode("utf-8", errors="backslashreplace")
                        line_timestamp, message = self.parse_log_line(line)
                        if line_timestamp:  # detect new log line
                            if message_to_log is None:  # first line in the log
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines.append(line)
                            else:  # previous log line is complete
                                for line in progress_callback_lines:
                                    for callback in self._callbacks:
                                        callback.progress_callback(
                                            line=line, client=self._client, mode=ExecutionMode.SYNC
                                        )
                                if message_to_log is not None:
                                    if is_log_group_marker(message_to_log):
                                        print(message_to_log)
                                    else:
                                        self.log.info("[%s] %s", container_name, message_to_log)
                                last_captured_timestamp = message_timestamp
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines = [line]
                        else:  # continuation of the previous log line
                            message_to_log = f"{message_to_log}\n{message}"
                            progress_callback_lines.append(line)
                finally:
                    # log the last line and update the last_captured_timestamp
                    for line in progress_callback_lines:
                        for callback in self._callbacks:
                            callback.progress_callback(
                                line=line, client=self._client, mode=ExecutionMode.SYNC
                            )
                    if message_to_log is not None:
                        if is_log_group_marker(message_to_log):
                            print(message_to_log)
                        else:
                            self.log.info("[%s] %s", container_name, message_to_log)
                    last_captured_timestamp = message_timestamp
            except TimeoutError as e:
                # in case of timeout, increment return time by 2 seconds to avoid
                # duplicate log entries
                if val := (last_captured_timestamp or since_time):
                    return val.add(seconds=2), e
            except HTTPError as e:
                exception = e
                self.log.exception(
                    "Reading of logs interrupted for container %r; will retry.",
                    container_name,
                )
            return last_captured_timestamp or since_time, exception

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        last_log_time = since_time
        while True:
            last_log_time, exc = consume_logs(since_time=last_log_time)
            if not self.container_is_running(pod, container_name=container_name):
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            # a timeout is a normal thing and we ignore it and resume following logs
            if not isinstance(exc, TimeoutError):
                self.log.warning(
                    "Pod %s log read interrupted but container %s still running. Logs generated in the last one second might get duplicated.",
                    pod.metadata.name,
                    container_name,
                )
            time.sleep(1)

    def _reconcile_requested_log_containers(
        self, requested: Iterable[str] | str | bool | None, actual: list[str], pod_name
    ) -> list[str]:
        """Return actual containers based on requested."""
        containers_to_log = []
        if actual:
            if isinstance(requested, str):
                # fetch logs only for requested container if only one container is provided
                if requested in actual:
                    containers_to_log.append(requested)
                else:
                    self.log.error(
                        "container %s whose logs were requested not found in the pod %s",
                        requested,
                        pod_name,
                    )
            elif isinstance(requested, bool):
                # if True is provided, get logs for all the containers
                if requested is True:
                    containers_to_log.extend(actual)
                else:
                    self.log.error(
                        "False is not a valid value for container_logs",
                    )
            else:
                # if a sequence of containers are provided, iterate for every container in the pod
                if isinstance(requested, Iterable):
                    for container in requested:
                        if container in actual:
                            containers_to_log.append(container)
                        else:
                            self.log.error(
                                "Container %s whose logs were requests not found in the pod %s",
                                container,
                                pod_name,
                            )
                else:
                    self.log.error(
                        "Invalid type %s specified for container names input parameter", type(requested)
                    )
        else:
            self.log.error("Could not retrieve containers for the pod: %s", pod_name)
        return containers_to_log

    def fetch_requested_init_container_logs(
        self, pod: V1Pod, init_containers: Iterable[str] | str | Literal[True] | None, follow_logs=False
    ) -> list[PodLoggingStatus]:
        """
        Follow the logs of containers in the specified pod and publish it to airflow logging.

        Returns when all the containers exit.

        :meta private:
        """
        pod_logging_statuses = []
        all_containers = self.get_init_container_names(pod)
        containers_to_log = self._reconcile_requested_log_containers(
            requested=init_containers,
            actual=all_containers,
            pod_name=pod.metadata.name,
        )
        # sort by spec.initContainers because containers runs sequentially
        containers_to_log = sorted(containers_to_log, key=lambda cn: all_containers.index(cn))
        for c in containers_to_log:
            self._await_init_container_start(pod=pod, container_name=c)
            status = self.fetch_container_logs(pod=pod, container_name=c, follow=follow_logs)
            pod_logging_statuses.append(status)
        return pod_logging_statuses

    def fetch_requested_container_logs(
        self, pod: V1Pod, containers: Iterable[str] | str | Literal[True], follow_logs=False
    ) -> list[PodLoggingStatus]:
        """
        Follow the logs of containers in the specified pod and publish it to airflow logging.

        Returns when all the containers exit.

        :meta private:
        """
        pod_logging_statuses = []
        all_containers = self.get_container_names(pod)
        containers_to_log = self._reconcile_requested_log_containers(
            requested=containers,
            actual=all_containers,
            pod_name=pod.metadata.name,
        )
        for c in containers_to_log:
            status = self.fetch_container_logs(pod=pod, container_name=c, follow=follow_logs)
            pod_logging_statuses.append(status)
        return pod_logging_statuses

    def await_container_completion(self, pod: V1Pod, container_name: str, polling_time: float = 1) -> None:
        """
        Wait for the given container in the given pod to be completed.

        :param pod: pod spec that will be monitored
        :param container_name: name of the container within the pod to monitor
        :param polling_time: polling time between two container status checks.
            Defaults to 1s.
        """
        while True:
            remote_pod = self.read_pod(pod)
            terminated = container_is_completed(remote_pod, container_name)
            if terminated:
                break
            self.log.info("Waiting for container '%s' state to be completed", container_name)
            time.sleep(polling_time)

    def await_pod_completion(
        self, pod: V1Pod, istio_enabled: bool = False, container_name: str = "base"
    ) -> V1Pod:
        """
        Monitor a pod and return the final state.

        :param istio_enabled: whether istio is enabled in the namespace
        :param pod: pod spec that will be monitored
        :param container_name: name of the container within the pod
        :return: tuple[State, str | None]
        """
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase in PodPhase.terminal_states:
                break
            if istio_enabled and container_is_completed(remote_pod, container_name):
                break
            self.log.info("Pod %s has phase %s", pod.metadata.name, remote_pod.status.phase)
            time.sleep(2)
        return remote_pod

    def parse_log_line(self, line: str) -> tuple[DateTime | None, str]:
        """
        Parse K8s log line and returns the final state.

        :param line: k8s log line
        :return: timestamp and log message
        """
        timestamp, sep, message = line.strip().partition(" ")
        if not sep:
            return None, line
        try:
            last_log_time = cast("DateTime", pendulum.parse(timestamp))
        except ParserError:
            return None, line
        return last_log_time, message

    def container_is_running(self, pod: V1Pod, container_name: str) -> bool:
        """Read pod and checks if container is running."""
        remote_pod = self.read_pod(pod)
        return container_is_running(pod=remote_pod, container_name=container_name)

    def container_is_terminated(self, pod: V1Pod, container_name: str) -> bool:
        """Read pod and checks if container is terminated."""
        remote_pod = self.read_pod(pod)
        return container_is_terminated(pod=remote_pod, container_name=container_name)

    @tenacity.retry(stop=tenacity.stop_after_attempt(6), wait=tenacity.wait_exponential(max=15), reraise=True)
    def read_pod_logs(
        self,
        pod: V1Pod,
        container_name: str,
        tail_lines: int | None = None,
        timestamps: bool = False,
        since_seconds: int | None = None,
        follow=True,
        post_termination_timeout: int = 120,
        **kwargs,
    ) -> PodLogsConsumer:
        """Read log from the POD."""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs["since_seconds"] = since_seconds

        if tail_lines:
            additional_kwargs["tail_lines"] = tail_lines
        additional_kwargs.update(**kwargs)

        try:
            logs = self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=container_name,
                follow=follow,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except HTTPError:
            self.log.exception("There was an error reading the kubernetes API.")
            raise

        return PodLogsConsumer(
            response=logs,
            pod=pod,
            pod_manager=self,
            container_name=container_name,
            post_termination_timeout=post_termination_timeout,
        )

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def get_init_container_names(self, pod: V1Pod) -> list[str]:
        """
        Return container names from the POD except for the airflow-xcom-sidecar container.

        :meta private:
        """
        return [container_spec.name for container_spec in pod.spec.init_containers]

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def get_container_names(self, pod: V1Pod) -> list[str]:
        """
        Return container names from the POD except for the airflow-xcom-sidecar container.

        :meta private:
        """
        pod_info = self.read_pod(pod)
        return [
            container_spec.name
            for container_spec in pod_info.spec.containers
            if container_spec.name != PodDefaults.SIDECAR_CONTAINER_NAME
        ]

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_events(self, pod: V1Pod) -> CoreV1EventList:
        """Read events from the POD."""
        try:
            return self._client.list_namespaced_event(
                namespace=pod.metadata.namespace, field_selector=f"involvedObject.name={pod.metadata.name}"
            )
        except HTTPError as e:
            raise AirflowException(f"There was an error reading the kubernetes API: {e}")

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod(self, pod: V1Pod) -> V1Pod:
        """Read POD information."""
        try:
            return self._client.read_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
        except HTTPError as e:
            raise AirflowException(f"There was an error reading the kubernetes API: {e}")

    def await_xcom_sidecar_container_start(
        self, pod: V1Pod, timeout: int = 900, log_interval: int = 30
    ) -> None:
        """Check if the sidecar container has reached the 'Running' state before performing do_xcom_push."""
        self.log.info("Checking if xcom sidecar container is started.")
        start_time = time.time()
        last_log_time = start_time

        while True:
            elapsed_time = time.time() - start_time
            if self.container_is_running(pod, PodDefaults.SIDECAR_CONTAINER_NAME):
                self.log.info("The xcom sidecar container has started.")
                break
            if self.container_is_terminated(pod, PodDefaults.SIDECAR_CONTAINER_NAME):
                raise AirflowException(
                    "Xcom sidecar container is already terminated! Not possible to read xcom output of task."
                )
            if (time.time() - last_log_time) >= log_interval:
                self.log.warning(
                    "Still waiting for the xcom sidecar container to start. Elapsed time: %d seconds.",
                    int(elapsed_time),
                )
                last_log_time = time.time()
            if elapsed_time > timeout:
                raise AirflowException(
                    f"Xcom sidecar container did not start within {timeout // 60} minutes."
                )
            time.sleep(1)

    def extract_xcom(self, pod: V1Pod) -> str:
        """Retrieve XCom value and kill xcom sidecar container."""
        try:
            result = self.extract_xcom_json(pod)
            return result
        finally:
            self.extract_xcom_kill(pod)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def extract_xcom_json(self, pod: V1Pod) -> str:
        """Retrieve XCom value and also check if xcom json is valid."""
        command = (
            f"if [ -s {PodDefaults.XCOM_MOUNT_PATH}/return.json ]; "
            f"then cat {PodDefaults.XCOM_MOUNT_PATH}/return.json; "
            f"else echo {EMPTY_XCOM_RESULT}; fi"
        )
        with closing(
            kubernetes_stream(
                self._client.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                pod.metadata.namespace,
                container=PodDefaults.SIDECAR_CONTAINER_NAME,
                command=[
                    "/bin/sh",
                    "-c",
                    command,
                ],
                stdin=False,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
        ) as client:
            self.log.info("Running command... %s", command)
            client.run_forever()
            if client.peek_stderr():
                stderr = client.read_stderr()
                self.log.error("stderr from command: %s", stderr)
            result = client.read_all()
            if result and result.rstrip() != EMPTY_XCOM_RESULT:
                # Note: result string is parsed to check if its valid json.
                # This function still returns a string which is converted into json in the calling method.
                json.loads(result)

        if result is None:
            raise AirflowException(f"Failed to extract xcom from pod: {pod.metadata.name}")
        return result

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def extract_xcom_kill(self, pod: V1Pod):
        """Kill xcom sidecar container."""
        with closing(
            kubernetes_stream(
                self._client.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                pod.metadata.namespace,
                container=PodDefaults.SIDECAR_CONTAINER_NAME,
                command=["/bin/sh"],
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
        ) as resp:
            self._exec_pod_command(resp, "kill -2 $(pgrep -u $(id -u) -f 'sh')")

    def _exec_pod_command(self, resp, command: str) -> str | None:
        res = ""
        if not resp.is_open():
            return None
        self.log.info("Running command... %s", command)
        resp.write_stdin(f"{command}\n")
        while resp.is_open():
            resp.update(timeout=1)
            while resp.peek_stdout():
                res += resp.read_stdout()
            error_res = ""
            while resp.peek_stderr():
                error_res += resp.read_stderr()
            if error_res:
                self.log.info("stderr from command: %s", error_res)
                break
            if res:
                return res
        return None

    def _await_init_container_start(self, pod: V1Pod, container_name: str):
        while True:
            remote_pod = self.read_pod(pod)

            if (
                remote_pod.status is not None
                and remote_pod.status.phase != PodPhase.PENDING
                and get_container_status(remote_pod, container_name) is not None
                and not container_is_wait(remote_pod, container_name)
            ):
                return

            time.sleep(1)


class OnFinishAction(str, enum.Enum):
    """Action to take when the pod finishes."""

    KEEP_POD = "keep_pod"
    DELETE_POD = "delete_pod"
    DELETE_SUCCEEDED_POD = "delete_succeeded_pod"


def is_log_group_marker(line: str) -> bool:
    """Check if the line is a log group marker like `::group::` or `::endgroup::`."""
    return line.startswith("::group::") or line.startswith("::endgroup::")
