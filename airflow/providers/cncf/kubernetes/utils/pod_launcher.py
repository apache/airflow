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
from contextlib import closing
from datetime import datetime as dt
from typing import Iterable, Optional, Tuple, Union

import pendulum
import tenacity
from kubernetes import client, watch
from kubernetes.client.models.v1_event_list import V1EventList
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from pendulum import Date, DateTime, Duration, Time
from pendulum.parsing.exceptions import ParserError
from requests.exceptions import BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin


class PodLaunchFailedException(AirflowException):
    """When pod launching fails in KubernetesPodOperator."""


def should_retry_start_pod(exception: Exception) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying"""
    if isinstance(exception, ApiException):
        return exception.status == 409
    return False


class PodStatus:
    """Status of the PODs"""

    PENDING = 'Pending'
    RUNNING = 'Running'
    FAILED = 'Failed'
    SUCCEEDED = 'Succeeded'

    terminal_states = {FAILED, SUCCEEDED}


def container_is_running(pod: V1Pod, container_name: str) -> bool:
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if not container_statuses:
        return False
    container_status = next(iter([x for x in container_statuses if x.name == container_name]), None)
    if not container_status:
        return False
    return container_status.state.running is not None


class PodLauncher(LoggingMixin):
    """Launches PODS"""

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
        pod_mutation_hook(pod)

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
        curr_time = dt.now()
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase != PodStatus.PENDING:
                break
            self.log.warning("Pod not yet started: %s", pod.metadata.name)
            delta = dt.now() - curr_time
            if delta.total_seconds() >= startup_timeout:
                msg = (
                    f"Pod took longer than {startup_timeout} seconds to start. "
                    "Check the pod events in kubernetes to determine why."
                )
                raise PodLaunchFailedException(msg)
            time.sleep(1)

    def follow_container_logs(self, pod: V1Pod, container_name: str):
        """
        Follows the logs of container and streams to airflow logging.
        Returns when container exits.
        """
        container_stopped = False
        read_logs_since_sec = None
        last_log_time = None

        # `read_pod_logs` follows the logs so we shouldn't necessarily _need_ to loop
        # but in a long-running process we might lose connectivity and this way we
        # can resume following the logs
        while True:
            try:
                logs = self.read_pod_logs(
                    pod=pod,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=read_logs_since_sec,
                )
                for line in logs:  # type: bytes
                    timestamp, message = self.parse_log_line(line.decode('utf-8'))
                    self.log.info(message)
                    if timestamp:
                        last_log_time = timestamp
            except BaseHTTPError:  # Catches errors like ProtocolError(TimeoutError).
                self.log.warning(
                    'Failed to read logs for pod %s',
                    pod.metadata.name,
                    exc_info=True,
                )

            if container_stopped:
                break

            if last_log_time:
                delta = pendulum.now() - last_log_time
                read_logs_since_sec = math.ceil(delta.total_seconds())

            time.sleep(1)

            if self.container_is_running(pod, container_name=container_name):
                self.log.info('Container %s is running', pod.metadata.name)
                self.log.warning('Pod %s log read interrupted', pod.metadata.name)
            else:
                container_stopped = True  # fetch logs once more and exit

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
            if remote_pod.status.phase in PodStatus.terminal_states:
                break
            self.log.info('Pod %s has phase %s', pod.metadata.name, remote_pod.status.phase)
            time.sleep(2)
        return remote_pod

    def parse_log_line(self, line: str) -> Tuple[Optional[Union[Date, Time, DateTime, Duration]], str]:
        """
        Parse K8s log line and returns the final state

        :param line: k8s log line
        :type line: str
        :return: timestamp and log message
        :rtype: Tuple[str, str]
        """
        split_at = line.find(' ')
        if split_at == -1:
            raise Exception(f'Log not in "{{timestamp}} {{log}}" format. Got: {line}')
        timestamp = line[:split_at]
        message = line[split_at + 1 :].rstrip()
        try:
            last_log_time = pendulum.parse(timestamp)
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
    ) -> Iterable[str]:
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
                follow=True,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception('There was an error reading the kubernetes API.')
            raise

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_events(self, pod: V1Pod) -> V1EventList:
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
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    return resp.read_stdout()
                if resp.peek_stderr():
                    self.log.info("stderr from command: %s", resp.read_stderr())
                    break
        return None
