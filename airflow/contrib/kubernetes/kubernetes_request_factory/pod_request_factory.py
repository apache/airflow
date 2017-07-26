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

import kubernetes_request_factory as kreq
import yaml
from airflow import AirflowException


class SimplePodRequestFactory(kreq.KubernetesRequestFactory):
    """
        Request generator for a simple pod.
    """
    _yaml = """apiVersion: v1
kind: Pod
metadata:
  name: name
spec:
  containers:
    - name: base
      image: airflow-slave:latest
      command: ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
      volumeMounts:
        - name: shared-data
          mountPath: "/usr/local/airflow/dags"
  restartPolicy: Never
    """

    def __init__(self):
        pass

    def create(self, pod):
        req = yaml.load(self._yaml)
        kreq.extract_name(pod, req)
        kreq.extract_labels(pod, req)
        kreq.extract_image(pod, req)
        kreq.extract_cmds(pod, req)
        if len(pod.node_selectors) > 0:
            kreq.extract_node_selector(pod, req)
        kreq.extract_secrets(pod, req)
        kreq.extract_volume_secrets(pod, req)
        kreq.attach_volume_mounts(req)
        return req


class ReturnValuePodRequestFactory(SimplePodRequestFactory):
    """
    Pod request factory with a PreStop hook to upload return value
    to the system's etcd service.
    :param kube_com_service_factory: Kubernetes Communication Service factory
    :type kube_com_service_factory: () => KubernetesCommunicationService
    """

    def __init__(self, kube_com_service_factory, result_data_file):
        super(ReturnValuePodRequestFactory, self).__init__()
        self._kube_com_service_factory = kube_com_service_factory
        self._result_data_file = result_data_file

    def after_create(self, body, pod):
        """
            Augment the pod with hyper-parameterized specific logic
            Adds a Kubernetes PreStop hook to upload the model training
            metrics to the Kubernetes communication engine (probably
            an etcd service running with airflow)
        """
        container = body['spec']['containers'][0]
        pre_stop_hook = self._kube_com_service_factory() \
            .pod_pre_stop_hook(self._result_data_file, pod.name)
        # Pre-stop hook only works on containers that are deleted. If the container
        # naturally exists there would be no pre-stop hook execution. Therefore we
        # simulate the hook by wrapping the exe command inside a script
        if "'" in ' '.join(container['command']):
            raise AirflowException('Please do not include single quote '
                                   'in your command for hyperparameterized pods')
        cmd = ' '.join(["'" + c + "'" if " " in c else c for c in container['command']])
        container['command'] = ['/bin/bash', '-c', "({}) ; ({})"
            .format(cmd, pre_stop_hook)]
