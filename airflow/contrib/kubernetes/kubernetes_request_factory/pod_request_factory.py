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
from airflow.contrib.kubernetes.pod import Pod
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
        # type: (Pod) -> dict
        req = yaml.load(self._yaml)
        kreq.extract_name(pod, req)
        kreq.extract_labels(pod, req)
        kreq.extract_image(pod, req)
        kreq.extract_cmds(pod, req)
        kreq.extract_args(pod, req)
        if len(pod.node_selectors) > 0:
            self.extract_node_selector(pod, req)
        self.extract_secrets(pod, req)
        self.extract_volume_secrets(pod, req)
        self.attach_volume_mounts(req=req, pod=pod)
        return req
