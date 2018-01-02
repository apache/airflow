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

import json
import time
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from datetime import datetime as dt
from airflow.contrib.kubernetes.kubernetes_request_factory import \
    pod_request_factory as pod_fac
from kubernetes import watch
from kubernetes.client.rest import ApiException
from airflow import AirflowException
from requests.exceptions import HTTPError
from .kube_client import get_kube_client


class PodStatus(object):
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    def __init__(self, kube_client=None):
        super(PodLauncher, self).__init__()
        self._client = kube_client or get_kube_client()
        self._watch = watch.Watch()
        self.kube_req_factory = pod_fac.SimplePodRequestFactory()

    def run_pod_async(self, pod):
        req = self.kube_req_factory.create(pod)
        self.log.debug('Pod Creation Request: \n{}'.format(json.dumps(req, indent=2)))
        try:
            resp = self._client.create_namespaced_pod(body=req, namespace=pod.namespace)
            self.log.debug('Pod Creation Response: {}'.format(resp))
        except ApiException:
            self.log.exception('Exception when attempting to create Namespaced Pod.')
            raise
        return resp

    def run_pod(self, pod, startup_timeout=120):
        # type: (Pod) -> State
        """
        Launches the pod synchronously and waits for completion.

        Args:
            pod (Pod):
            startup_timeout (int): Timeout for startup of the pod (if pod is pending for
             too long, considers task a failure
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                delta = dt.now() - curr_time
                if delta.seconds >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(1)
            self.log.debug('Pod not yet started')

        final_status = self._monitor_pod(pod)
        return final_status

    def _monitor_pod(self, pod):
        # type: (Pod) -> State

        while self.pod_is_running(pod):
            self.log.info("Pod {} has state {}".format(pod.name, State.RUNNING))
            time.sleep(2)
        return self._task_status(self.read_pod(pod))

    def _task_status(self, event):
        # type: (V1Pod) -> State
        self.log.info(
            "Event: {} had an event of type {}".format(event.metadata.name,
                                                       event.status.phase))
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod):
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod):
        state = self._task_status(self.read_pod(pod))
        return state != State.SUCCESS and state != State.FAILED

    def read_pod(self, pod):
        try:
            return self._client.read_namespaced_pod(pod.name, pod.namespace)
        except HTTPError as e:
            raise AirflowException("There was an error reading the kubernetes API: {}"
                                   .format(e))

    def process_status(self, job_id, status):
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.info("Event: {} Failed".format(job_id))
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info("Event: {} Succeeded".format(job_id))
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.info("Event: Invalid state {} on job {}".format(status, job_id))
            return State.FAILED
