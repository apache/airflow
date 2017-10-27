# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.kubernetes_request_factory.pod_request_factory import (
    SimplePodRequestFactory)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from kubernetes import watch
from kubernetes.client import V1Pod
from kubernetes.client.rest import ApiException

from .kube_client import get_kube_client


class PodStatus(object):
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    def __init__(self, kube_client=None):
        self.kube_req_factory = SimplePodRequestFactory()
        self._client = kube_client or get_kube_client()
        self._watch = watch.Watch()

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

    def run_pod(self, pod):
        # type: (Pod) -> State
        """
        Launches the pod synchronously and waits for completion.
        """
        resp = self.run_pod_async(pod)
        final_status = self._monitor_pod(pod)
        return final_status

    def _monitor_pod(self, pod):
        # type: (Pod) -> State
        for event in self._watch.stream(self.read_pod(pod), pod.namespace):
            status = self._task_status(event)
            if status == State.SUCCESS or status == State.FAILED:
                return status

    def _task_status(self, event):
        # type: (V1Pod) -> State
        task = event['object']
        self.log.info(
            "Event: {} had an event of type {}".format(task.metadata.name,
                                                       event['type']))
        status = self.process_status(task.metadata.name, task.status.phase)
        return status

    def read_pod(self, pod):
        return self._client.read_namespaced_pod(pod.name, pod.namespace)

    def process_status(self, job_id, status):
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
