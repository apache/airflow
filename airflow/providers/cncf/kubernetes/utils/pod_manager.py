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
"""Launches PODs"""
import json
import math
import time
import warnings
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Optional, Tuple, cast

import pendulum
import tenacity
from kubernetes import client, watch
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError
from urllib3.exceptions import HTTPError as BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from kubernetes.client.models.core_v1_event_list import CoreV1EventList


class PodLaunchFailedException(AirflowException):
    """When pod launching fails in KubernetesPodOperator."""


def should_retry_start_pod(exception: BaseException) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying"""
    if isinstance(exception, ApiException):
        return exception.status == 409
    return False


class PodPhase:
    """
    Possible pod phases
    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    PENDING = 'Pending'
    RUNNING = 'Running'
    FAILED = 'Failed'
    SUCCEEDED = 'Succeeded'

    terminal_states = {FAILED, SUCCEEDED}


def container_is_running(pod: V1Pod, container_name: str) -> bool:
    """
    Examines V1Pod ``pod`` to determine whether ``container_name`` is running.
    If that container is present and running, returns True.  Returns False otherwise.
    """
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if not container_statuses:
        return False
    container_status = next(iter([x for x in container_statuses if x.name == container_name]), None)
    if not container_status:
        return False
    return container_status.state.running is not None


def get_container_termination_message(pod: V1Pod, container_name: str):
    try:
        container_statuses = pod.status.container_statuses
        container_status = next(iter([x for x in container_statuses if x.name == container_name]), None)
        return container_status.state.terminated.message if container_status else None
    except (AttributeError, TypeError):
        return None


@dataclass
class PodLoggingStatus:
    """Used for returning the status of the pod and last log time when exiting from `fetch_container_logs`"""

    running: bool
    last_log_time: Optional[DateTime]


class PodManager(LoggingMixin):
    """
    Helper class for creating, monitoring, and otherwise interacting with Kubernetes pods
    for use with the KubernetesPodOperator
    """

    def __init__(
        self,
        kube_client: client.CoreV1Api = None,
        in_cluster: bool = True,
        cluster_context: Optional[str] = None,
    ):
        """
        Creates the launcher.

        :param kube_client: kubernetes client
        :param in_cluster: whether we are in cluster
        :param cluster_context: context of the cluster
        """
        super().__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster, cluster_context=cluster_context)
        self._watch = watch.Watch()

    def run_pod_async(self, pod: V1Pod, **kwargs) -> V1Pod:
        """Runs POD asynchronously"""
        sanitized_pod = self._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug('Pod Creation Request: \n%s', json_pod)
        try:
            resp = self._client.create_namespaced_pod(
                body=sanitized_pod, namespace=pod.metadata.namespace, **kwargs
            )
            self.log.debug('Pod Creation Response: %s', resp)
        except Exception as e:
            self.log.exception(
                'Exception when attempting to create Namespaced Pod: %s', str(json_pod).replace("\n", " ")
            )
            raise e
        return resp

    def delete_pod(self, pod: V1Pod) -> None:
        """Deletes POD"""
        try:
            self._client.delete_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace, body=client.V1DeleteOptions()
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_pod),
    )
    def create_pod(self, pod: V1Pod) -> V1Pod:
        """Launches the pod asynchronously."""
        return self.run_pod_async(pod)

    def await_pod_start(self, pod: V1Pod, startup_timeout: int = 120) -> None:
        """
        Waits for the pod to reach phase other than ``Pending``

        :param pod:
        :param startup_timeout: Timeout (in seconds) for startup of the pod
            (if pod is pending for too long, fails task)
        :return:
        """
        curr_time = datetime.now()
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase != PodPhase.PENDING:
                break
            self.log.warning("Pod not yet started: %s", pod.metadata.name)
            delta = datetime.now() - curr_time
            if delta.total_seconds() >= startup_timeout:
                msg = (
                    f"Pod took longer than {startup_timeout} seconds to start. "
                    "Check the pod events in kubernetes to determine why."
                )
                raise PodLaunchFailedException(msg)
            time.sleep(1)

    def follow_container_logs(self, pod: V1Pod, container_name: str) -> PodLoggingStatus:
        warnings.warn(
            "Method `follow_container_logs` is deprecated.  Use `fetch_container_logs` instead"
            "with option `follow=True`.",
            DeprecationWarning,
        )
        return self.fetch_container_logs(pod=pod, container_name=container_name, follow=True)

    def fetch_container_logs(
        self, pod: V1Pod, container_name: str, *, follow=False, since_time: Optional[DateTime] = None
    ) -> PodLoggingStatus:
        """
        Follows the logs of container and streams to airflow logging.
        Returns when container exits.
        """

        def consume_logs(*, since_time: Optional[DateTime] = None, follow: bool = True) -> Optional[DateTime]:
            """
            Tries to follow container logs until container completes.
            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """
            timestamp = None
            try:
                logs = self.read_pod_logs(
                    pod=pod,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=(
                        math.ceil((pendulum.now() - since_time).total_seconds()) if since_time else None
                    ),
                    follow=follow,
                )
                for raw_line in logs:
                    line = raw_line.decode('utf-8', errors="backslashreplace")
                    timestamp, message = self.parse_log_line(line)
                    self.log.info(message)
            except BaseHTTPError as e:
                self.log.warning(
                    "Reading of logs interrupted with error %r; will retry. "
                    "Set log level to DEBUG for traceback.",
                    e,
                )
                self.log.debug(
                    "Traceback for interrupted logs read for pod %r",
                    pod.metadata.name,
                    exc_info=True,
                )
            return timestamp or since_time

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        last_log_time = since_time
        while True:
            last_log_time = consume_logs(since_time=last_log_time, follow=follow)
            if not self.container_is_running(pod, container_name=container_name):
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            else:
                self.log.warning(
                    'Pod %s log read interrupted but container %s still running',
                    pod.metadata.name,
                    container_name,
                )
                time.sleep(1)

    def await_container_completion(self, pod: V1Pod, container_name: str) -> None:
        while not self.container_is_running(pod=pod, container_name=container_name):
            time.sleep(1)

    def await_pod_completion(self, pod: V1Pod) -> V1Pod:
        """
        Monitors a pod and returns the final state

        :param pod: pod spec that will be monitored
        :return:  Tuple[State, Optional[str]]
        """
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase in PodPhase.terminal_states:
                break
            self.log.info('Pod %s has phase %s', pod.metadata.name, remote_pod.status.phase)
            time.sleep(2)
        return remote_pod

    def parse_log_line(self, line: str) -> Tuple[Optional[DateTime], str]:
        """
        Parse K8s log line and returns the final state

        :param line: k8s log line
        :return: timestamp and log message
        :rtype: Tuple[str, str]
        """
        split_at = line.find(' ')
        if split_at == -1:
            self.log.error(
                "Error parsing timestamp (no timestamp in message %r). "
                "Will continue execution but won't update timestamp",
                line,
            )
            return None, line
        timestamp = line[:split_at]
        message = line[split_at + 1 :].rstrip()
        try:
            last_log_time = cast(DateTime, pendulum.parse(timestamp))
        except ParserError:
            self.log.error("Error parsing timestamp. Will continue execution but won't update timestamp")
            return None, line
        return last_log_time, message

    def container_is_running(self, pod: V1Pod, container_name: str) -> bool:
        """Reads pod and checks if container is running"""
        remote_pod = self.read_pod(pod)
        return container_is_running(pod=remote_pod, container_name=container_name)

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_logs(
        self,
        pod: V1Pod,
        container_name: str,
        tail_lines: Optional[int] = None,
        timestamps: bool = False,
        since_seconds: Optional[int] = None,
        follow=True,
    ) -> Iterable[bytes]:
        """Reads log from the POD"""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs['since_seconds'] = since_seconds

        if tail_lines:
            additional_kwargs['tail_lines'] = tail_lines

        try:
            return self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=container_name,
                follow=follow,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception('There was an error reading the kubernetes API.')
            raise

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_events(self, pod: V1Pod) -> "CoreV1EventList":
        """Reads events from the POD"""
        try:
            return self._client.list_namespaced_event(
                namespace=pod.metadata.namespace, field_selector=f"involvedObject.name={pod.metadata.name}"
            )
        except BaseHTTPError as e:
            raise AirflowException(f'There was an error reading the kubernetes API: {e}')

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod(self, pod: V1Pod) -> V1Pod:
        """Read POD information"""
        try:
            return self._client.read_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
        except BaseHTTPError as e:
            raise AirflowException(f'There was an error reading the kubernetes API: {e}')

    def extract_xcom(self, pod: V1Pod) -> str:
        """Retrieves XCom value and kills xcom sidecar container"""
        with closing(
            kubernetes_stream(
                self._client.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                pod.metadata.namespace,
                container=PodDefaults.SIDECAR_CONTAINER_NAME,
                command=['/bin/sh'],
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
        ) as resp:
            result = self._exec_pod_command(resp, f'cat {PodDefaults.XCOM_MOUNT_PATH}/return.json')
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        if result is None:
            raise AirflowException(f'Failed to extract xcom from pod: {pod.metadata.name}')
        return result

    def _exec_pod_command(self, resp, command: str) -> Optional[str]:
        res = None
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                while resp.peek_stdout():
                    res = res + resp.read_stdout() if res else resp.read_stdout()
                error_res = None
                while resp.peek_stderr():
                    error_res = error_res + resp.read_stderr() if error_res else resp.read_stderr()
                if error_res:
                    self.log.info("stderr from command: %s", error_res)
                    break
                if res:
                    return res
        return res
