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
from .kubernetes_request_factory import *


class SimpleJobRequestFactory(KubernetesRequestFactory):
    """
        Request generator for a simple pod.
    """

    def __init__(self):
        pass

    _yaml = """apiVersion: batch/v1
kind: Job
metadata:
  name: name
spec:
  template:
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

    def create(self, pod):
        req = yaml.load(self._yaml)
        sub_req = req['spec']['template']
        extract_name(pod, sub_req)
        extract_labels(pod, sub_req)
        extract_image(pod, sub_req)
        extract_cmds(pod, sub_req)
        if len(pod.node_selectors) > 0:
            extract_node_selector(pod, sub_req)
        extract_secrets(pod, sub_req)
        print("attaching volume mounts")
        attach_volume_mounts(sub_req)
        return req



