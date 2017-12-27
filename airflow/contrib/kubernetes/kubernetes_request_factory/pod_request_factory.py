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

import yaml
from airflow.contrib.kubernetes.kubernetes_request_factory.kubernetes_request_factory \
    import KubernetesRequestFactory


class SimplePodRequestFactory(KubernetesRequestFactory):
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
  restartPolicy: Never
    """

    def __init__(self):
        pass

    def create(self, pod):
        # type: (Pod) -> dict
        req = yaml.load(self._yaml)
        self.extract_name(pod, req)
        self.extract_labels(pod, req)
        self.extract_image(pod, req)
        self.extract_image_pull_policy(pod, req)
        self.extract_cmds(pod, req)
        self.extract_args(pod, req)
        self.extract_node_selector(pod, req)
        self.extract_volume_secrets(pod, req)
        self.attach_volumes(pod, req)
        self.attach_volume_mounts(pod, req)
        self.extract_resources(pod, req)
        return req
