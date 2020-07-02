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
import time
from datetime import datetime as dt

import tenacity
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from requests.exceptions import BaseHTTPError

from airflow import AirflowException
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from .kube_client import get_kube_client


class PodStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    """Launches PODS"""
    def __init__(self,
                 kube_client=None,
                 in_cluster=True,
                 cluster_context=None,
                 extract_xcom=False):
        """
        Creates the launcher.

        :param kube_client: kubernetes client
        :param in_cluster: whether we are in cluster
        :param cluster_context: context of the cluster
        :param extract_xcom: whether we should extract xcom
        """
        super(PodLauncher, self).__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster,
                                                      cluster_context=cluster_context)
        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom

    def run_pod_async(self, pod, **kwargs):
        """Runs POD asynchronously"""
        pod_mutation_hook(pod)

        sanitized_pod = self._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug('Pod Creation Request: \n%s', json_pod)
        try:
            resp = self._client.create_namespaced_pod(body=sanitized_pod,
                                                      namespace=pod.metadata.namespace, **kwargs)
            self.log.debug('Pod Creation Response: %s', resp)
        except Exception as e:
            self.log.exception('Exception when attempting '
                               'to create Namespaced Pod: %s', json_pod)
            raise e
        return resp

    def delete_pod(self, pod):
        """Deletes POD"""
        try:
            self._client.delete_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace, body=client.V1DeleteOptions())
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def start_pod(
            self,
            pod,
            startup_timeout):
        """
        Launches the pod synchronously and waits for completion.

        :param pod:
        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :return:
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                delta = dt.now() - curr_time
                if delta.total_seconds() >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(1)
            self.log.debug('Pod not yet started')

    def monitor_pod(self, pod, get_logs):
        """
        :param pod: pod spec that will be monitored
        :type pod : V1Pod
        :param get_logs: whether to read the logs locally
        :return:  Tuple[State, Optional[str]]
        """

        if get_logs:
            logs = self.read_pod_logs(pod)
            for line in logs:
                self.log.info(line)
        result = None
        if self.extract_xcom:
            while self.base_container_is_running(pod):
                self.log.info('Container %s has state %s', pod.metadata.name, State.RUNNING)
                time.sleep(2)
            result = self._extract_xcom(pod)
            self.log.info(result)
            result = json.loads(result)
        while self.pod_is_running(pod):
            self.log.info('Pod %s has state %s', pod.metadata.name, State.RUNNING)
            time.sleep(2)
        return self._task_status(self.read_pod(pod)), result

    def _task_status(self, event):
        self.log.info(
            'Event: %s had an event of type %s',
            event.metadata.name, event.status.phase)
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod):
        """Tests if pod has not started"""
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod):
        """Tests if pod is running"""
        state = self._task_status(self.read_pod(pod))
        return state not in (State.SUCCESS, State.FAILED)

    def base_container_is_running(self, pod):
        """Tests if base container is running"""
        event = self.read_pod(pod)
        status = next(iter(filter(lambda s: s.name == 'base',
                                  event.status.container_statuses)), None)
        if not status:
            return False
        return status.state.running is not None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_logs(self, pod, tail_lines=10):
        """Reads log from the POD"""
        try:
            return self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container='base',
                follow=True,
                tail_lines=tail_lines,
                _preload_content=False
            )
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_events(self, pod):
        """Reads events from the POD"""
        try:
            return self._client.list_namespaced_event(
                namespace=pod.metadata.namespace,
                field_selector="involvedObject.name={}".format(pod.metadata.name)
            )
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod(self, pod):
        """Read POD information"""
        try:
            return self._client.read_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    def _extract_xcom(self, pod):
        resp = kubernetes_stream(self._client.connect_get_namespaced_pod_exec,
                                 pod.metadata.name, pod.metadata.namespace,
                                 container=PodDefaults.SIDECAR_CONTAINER_NAME,
                                 command=['/bin/sh'], stdin=True, stdout=True,
                                 stderr=True, tty=False,
                                 _preload_content=False)
        try:
            result = self._exec_pod_command(
                resp, 'cat {}/return.json'.format(PodDefaults.XCOM_MOUNT_PATH))
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        finally:
            resp.close()
        if result is None:
            raise AirflowException('Failed to extract xcom from pod: {}'.format(pod.metadata.name))
        return result

    def _exec_pod_command(self, resp, command):
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    return resp.read_stdout()
                if resp.peek_stderr():
                    self.log.info(resp.read_stderr())
                    break
        return None

    def process_status(self, job_id, status):
        """Process status infomration for the JOB"""
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.info('Event with job id %s Failed', job_id)
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info('Event with job id %s Succeeded', job_id)
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.info('Event: Invalid state %s on job %s', status, job_id)
            return State.FAILED
