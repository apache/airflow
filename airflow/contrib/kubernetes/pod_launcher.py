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
import logging
import os
import time
import requests
import kubernetes

from airflow import AirflowException
from pod import Pod

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from kubernetes_request_factory import KubernetesRequestFactory, KubernetesRequestFactoryHelper


def kube_core_api():
    config.load_incluster_config()
    return client.CoreV1Api()


def incluster_namespace():
    """
    :return: Extracted in cluster namespace from kube config file
    """
    mounted_ns = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if not os.path.exists(mounted_ns):
       logging.error('Incluster namespace mount not found')
       raise AirflowException('Incluster namespace mount was not found!')
    with open(mounted_ns, 'rt') as f:
        ns = f.read()
        return ns


class KubernetesLauncher:
    """
    This class is responsible for launching objects to Kubernetes.
    Extend this class to launch exotic objects.
    Before trying to extend this method check if augmenting the request factory
    is enough for your use-case

    :param kube_object: A pod or anything that represents a Kubernetes object
    :type kube_object: Pod
    :param request_factory: A factory method to create kubernetes requests.
    """

    pod_timeout = 3600

    def __init__(self, kube_object, request_factory):
        if not isinstance(kube_object, Pod):
            raise Exception('`kube_object` must inherit from Pod')
        if not callable(request_factory):
            raise Exception('`request_factory` must be callable')
        the_request_factory = request_factory()
        if not isinstance(the_request_factory, KubernetesRequestFactory):
            raise Exception('`request_factory()` must inherit from KubernetesRequestFactory but is '
                            + str(type(the_request_factory)))
        self.pod = kube_object
        self.request_factory = the_request_factory

    def launch(self):
        """
            Launches the pod synchronously and waits for completion.
            No return value from execution. Will raise an exception if things failed
        """
        k8s_beta = kube_core_api()
        req = self.request_factory.create(self.pod)
        logging.info(json.dumps(req))
        self._delete_existing_pod(k8s_beta)
        resp = k8s_beta.create_namespaced_pod(body=req, namespace=self.pod.namespace or incluster_namespace())
        logging.info("Job created. status='%s', request:\n%s" % (str(resp.status), str(req)))
        for i in range(1, self.pod_timeout):
            time.sleep(10)
            logging.info('Waiting for success')
            if self._execution_finished():
                logging.info('Job finished!')
                return
        raise Exception("Job timed out!")

    def _execution_finished(self):
        k8s_beta = kube_core_api()
        resp = k8s_beta.read_namespaced_pod_status(
            KubernetesRequestFactoryHelper.sanitize_name(self.pod.name), namespace=self.pod.namespace)
        logging.info('status : ' + str(resp.status))
        logging.info('phase : ' + str(resp.status.phase))
        if resp.status.phase == 'Failed':
            raise Exception("Job " + self.pod.name + " failed!")
        return resp.status.phase != 'Running' and resp.status.phase != 'Pending'

    def _delete_existing_pod(self, k8client):
        logging.info('deleting pod ' + self.pod.name)
        try:
            resp = k8client.delete_namespaced_pod(
                name=KubernetesRequestFactoryHelper.sanitize_name(self.pod.name),
                namespace=self.pod.namespace,
                body=kubernetes.client.models.V1DeleteOptions())
            logging.info('delete result: ' + str(resp))
        except ApiException as e:
            if e.status == 404:
                logging.info('but there was nothing to delete')
            else:
                raise


class KubernetesCommunicationService:
    """
    A service that manages communications between pods in Kubernetes and ariflow dagrun
    Note that etcd service is running side by side of the airflow on the same machine
    using kubernetes magic, so on airflow side we use localhost, and on the remote side
    we use the provided etcd host.
    """

    def __init__(self, etcd_host, etcd_port):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.url = 'http://localhost:{}'.format(self.etcd_port)

    def pod_pre_stop_hook(self, return_data_file, task_id):
        return 'echo value=$(cat %s) | curl -d "@-" -X PUT %s:%s/v2/keys/pod_metrics/%s' \
               % (return_data_file, self.etcd_host, self.etcd_port, task_id)

    def pod_return_data(self, task_id):
        """
            Returns the pod's return data. The pod_pre_stop_hook is responsible to upload
            the return data to etcd.

            If the return_data_file is generated by the application, the pre stop hook
            will upload it to etcd and we will be download it back to airflow.
        """
        logging.info('querying {} for task id {}'.format(self.url, task_id))
        result = requests.get(self.url + '/v2/keys/pod_metrics/' + task_id)
        logging.info('result for querying {} for task id {}: {}'
                     .format(self.url, task_id, result.text))
        if result.status_code == 200:
            return json.loads(result.text)['node']['value']
        if result.status_code == 404:
            return None  # Data not found
        result.raise_for_status()

    @staticmethod
    def from_dag_default_args(dag):
        (etcd_host, etcd_port) = dag.default_args.get('etcd_endpoint', ':').split(':')
        logging.info('Setting etcd endpoint from dag default args {}:{}'
                     .format(etcd_host, etcd_port))
        if not etcd_host:
            raise Exception('`KubernetesCommunicationService` '
                            'requires etcd endpoint. Please defined it in dag '
                            'degault_args')
        return KubernetesCommunicationService(etcd_host, etcd_port)
