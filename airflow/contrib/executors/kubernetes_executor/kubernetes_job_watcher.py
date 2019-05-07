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

from airflow.exceptions import AirflowException
import multiprocessing
from kubernetes import watch, client
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.kubernetes.kube_client import get_kube_client


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin, object):
    def __init__(self, namespace, watcher_queue, resource_version, worker_uuid, kube_config):
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.worker_uuid = worker_uuid
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self):
        kube_client = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(kube_client, self.resource_version,
                                                  self.worker_uuid, self.kube_config)
            except Exception:
                self.log.exception('Unknown error in KubernetesJobWatcher. Failing')
                raise
            else:
                self.log.warn('Watch died gracefully, starting back up with: '
                              'last resource_version: %s', self.resource_version)

    def _run(self, kube_client, resource_version, worker_uuid, kube_config):
        self.log.info(
            'Event: and now my watch begins starting at resource_version: %s',
            resource_version
        )
        watcher = watch.Watch()

        kwargs = {'label_selector': 'airflow-worker={}'.format(worker_uuid)}
        if resource_version:
            kwargs['resource_version'] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.iteritems():
                kwargs[key] = value

        last_resource_version = None
        for event in watcher.stream(kube_client.list_namespaced_pod, self.namespace,
                                    **kwargs):
            task = event['object']
            self.log.info(
                'Event: %s had an event of type %s',
                task.metadata.name, event['type']
            )
            if event['type'] == 'ERROR':
                return self.process_error(event)
            self.process_status(
                task.metadata.name, task.status.phase, task.metadata.labels,
                task.metadata.resource_version
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event: dict):
        self.log.error(
            'Encountered Error response from k8s list namespaced pod stream => %s',
            event
        )
        raw_object = event['raw_object']
        if raw_object['code'] == 410:
            self.log.info(
                'Kubernetes resource version is too old, must reset to 0 => %s',
                raw_object['message']
            )
            # Return resource version 0
            return '0'
        raise AirflowException(
            'Kubernetes failure for %s with code %s and message: %s',
            raw_object['reason'], raw_object['code'], raw_object['message']
        )

    def process_status(self, pod_id: str, status: str, labels: list, resource_version: str):
        if status == 'Pending':
            self.log.info('Event: %s Pending', pod_id)
        elif status == 'Failed':
            self.log.info('Event: %s Failed', pod_id)
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.log.info('Event: %s Succeeded', pod_id)
            self.watcher_queue.put((pod_id, None, labels, resource_version))
        elif status == 'Running':
            self.log.info('Event: %s is Running', pod_id)
        else:
            self.log.warn(
                'Event: Invalid state: %s on pod: %s with labels: %s with '
                'resource_version: %s', status, pod_id, labels, resource_version
            )
