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
from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.kubernetes_request_factory import SimplePodRequestFactory
from kubernetes import config, client, watch
from kubernetes.client import V1Pod
from airflow.utils.state import State
import json
import logging


class PodLauncher:
    def __init__(self):
        self.kube_req_factory = SimplePodRequestFactory()
        self._client = self._kube_client()
        self._watch = watch.Watch()
        self.logger = logging.getLogger(__name__)

    def run_pod_async(self, pod):
        req = self.kube_req_factory.create(pod)
        print(json.dumps(req))
        resp = self._client.create_namespaced_pod(body=req, namespace=pod.namespace)
        return resp

    def run_pod(self, pod):
        # type: (Pod) -> State
        """
            Launches the pod synchronously and waits for completion.
        """
        resp = self.run_pod_async(pod)
        final_status = self._monitor_pod(pod)
        return final_status

    def _kube_client(self):
        #TODO: This should also allow people to point to a cluster.
        config.load_incluster_config()
        return client.CoreV1Api()

    def _monitor_pod(self, pod):
        # type: (Pod) -> State
        for event in self._watch.stream(self.read_pod(pod), pod.namespace):
            status = self._task_status(event)
            if status == State.SUCCESS or status == State.FAILED:
                return status

    def _task_status(self, event):
        # type: (V1Pod) -> State
        task = event['object']
        self.logger.info(
            "Event: {} had an event of type {}".format(task.metadata.name,
                                                       event['type']))
        status = self.process_status(task.metadata.name, task.status.phase)
        return status

    def read_pod(self, pod):
        return self._client.read_namespaced_pod(pod.name, pod.namespace)

    def process_status(self, job_id, status):
        if status == 'Pending':
            return State.QUEUED
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(job_id))
            return State.FAILED
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(job_id))
            return State.SUCCESS
        elif status == 'Running':
            return State.RUNNING
        else:
            self.logger.info("Event: Invalid state {} on job {}".format(status, job_id))
            return State.FAILED
