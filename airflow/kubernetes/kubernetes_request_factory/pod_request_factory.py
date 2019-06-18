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

from typing import Dict

import yaml
from airflow.kubernetes.pod import Pod
from airflow.kubernetes.kubernetes_request_factory.kubernetes_request_factory \
    import KubernetesRequestFactory


class SimplePodRequestFactory(KubernetesRequestFactory):
    """
    Request generator for a pod.

    """
    _yaml = """apiVersion: v1
kind: Pod
metadata:
  name: name
spec:
  containers:
    - name: base
      image: airflow-worker:latest
      command: ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
  restartPolicy: Never
    """

    def __init__(self):

        pass

    def create(self, pod: Pod) -> Dict:
        req = yaml.safe_load(self._yaml)
        self.extract_name(pod, req)
        self.extract_labels(pod, req)
        self.extract_image(pod, req)
        self.extract_image_pull_policy(pod, req)
        self.extract_cmds(pod, req)
        self.extract_args(pod, req)
        self.extract_node_selector(pod, req)
        self.extract_env_and_secrets(pod, req)
        self.extract_volume_secrets(pod, req)
        self.attach_ports(pod, req)
        self.attach_volumes(pod, req)
        self.attach_volume_mounts(pod, req)
        self.extract_resources(pod, req)
        self.extract_service_account_name(pod, req)
        self.extract_init_containers(pod, req)
        self.extract_image_pull_secrets(pod, req)
        self.extract_annotations(pod, req)
        self.extract_affinity(pod, req)
        self.extract_hostnetwork(pod, req)
        self.extract_tolerations(pod, req)
        self.extract_security_context(pod, req)
        return req


class ExtractXcomPodRequestFactory(KubernetesRequestFactory):
    """
    Request generator for a pod with sidecar container.

    """

    XCOM_MOUNT_PATH = '/airflow/xcom'
    SIDECAR_CONTAINER_NAME = 'airflow-xcom-sidecar'
    _yaml = """apiVersion: v1
kind: Pod
metadata:
  name: name
spec:
  volumes:
    - name: xcom
      emptyDir: {{}}
  containers:
    - name: base
      image: airflow-worker:latest
      command: ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
      volumeMounts:
        - name: xcom
          mountPath: {xcomMountPath}
    - name: {sidecarContainerName}
      image: python:3.5-alpine
      command:
        - python
        - -c
        - |
            import time
            while True:
                try:
                    time.sleep(3600)
                except KeyboardInterrupt:
                    exit(0)
      volumeMounts:
        - name: xcom
          mountPath: {xcomMountPath}
  restartPolicy: Never
    """.format(xcomMountPath=XCOM_MOUNT_PATH, sidecarContainerName=SIDECAR_CONTAINER_NAME)

    def __init__(self):
        pass

    def create(self, pod: Pod) -> Dict:
        req = yaml.safe_load(self._yaml)
        self.extract_name(pod, req)
        self.extract_labels(pod, req)
        self.extract_image(pod, req)
        self.extract_image_pull_policy(pod, req)
        self.extract_cmds(pod, req)
        self.extract_args(pod, req)
        self.extract_node_selector(pod, req)
        self.extract_env_and_secrets(pod, req)
        self.extract_volume_secrets(pod, req)
        self.attach_ports(pod, req)
        self.attach_volumes(pod, req)
        self.attach_volume_mounts(pod, req)
        self.extract_resources(pod, req)
        self.extract_service_account_name(pod, req)
        self.extract_init_containers(pod, req)
        self.extract_image_pull_secrets(pod, req)
        self.extract_annotations(pod, req)
        self.extract_affinity(pod, req)
        self.extract_hostnetwork(pod, req)
        self.extract_tolerations(pod, req)
        self.extract_security_context(pod, req)
        return req
