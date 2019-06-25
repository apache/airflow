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

from abc import ABCMeta, abstractmethod
import kubernetes.client.models as k8s
import copy


def merge_pods(base_pod: k8s.V1Pod, client_pod: k8s.V1Pod) -> k8s.V1Pod:
    """
    :param base_pod: has the base attributes which are overwritten if they exist
        in the client pod and remain if the do not exist in the client_pod
    :type base_pod: k8s.V1Pod
    :param client_pod: the pod that the client wants to create.
    :type client_pod: k8s.V1Pod
    :return: the merged pods

    This can't be done recursively as certain things are overwritten and certain
    things are added to, e.g. the [k8s.V1PodSpec] is overwritten in some spots but
    volumeMounts should be added to
    """

    client_pod_cp = copy.deepcopy(client_pod)

    def merge_objects(base_obj, client_obj):
        for base_key, base_val in base_obj.to_dict().items():
            if getattr(client_obj, base_key, None) is None and base_val is not None:
                setattr(client_obj, base_key, base_val)

    merge_objects(base_pod, client_pod_cp)
    merge_objects(base_pod.spec, client_pod_cp.spec)
    merge_objects(base_pod.metadata, client_pod_cp.metadata)
    merge_objects(base_pod.spec.containers[0], client_pod_cp.spec.containers[0])

    return client_pod_cp


class KubernetesRequestFactory(metaclass=ABCMeta):

    _base_pod = k8s.V1Pod(
        api_version='v1',
        kind='Pod',
        metadata=k8s.V1ObjectMeta(
            name='name',
            annotations={}
        ),
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(
                name='base',
                image='airflow-worker:latest',
                command=["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"],
                image_pull_policy='IfNotPresent'
            )],
            restart_policy='Never'
        )
    )

    @abstractmethod
    def create(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        pass


class SimplePodRequestFactory(KubernetesRequestFactory):

    def create(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        """
        Creates the request for kubernetes API.
        :param pod: The pod object
        """
        return merge_pods(self._base_pod, pod)


class ExtractXcomPodRequestFactory(SimplePodRequestFactory):
    """
    Request generator for a pod with sidecar container.

    """

    XCOM_MOUNT_PATH = '/airflow/xcom'
    SIDECAR_CONTAINER_NAME = 'airflow-xcom-sidecar'
    XCOM_CMD = """import time
    while True:
        try:
            time.sleep(3600)
        except KeyboardInterrupt:
            exit(0)
    """

    _volume_mount = k8s.V1VolumeMount(
        name='xcom',
        mount_path=XCOM_MOUNT_PATH
    )

    _volume = k8s.V1Volume(
        name='xcom',
        empty_dir=k8s.V1EmptyDirVolumeSource()
    )

    _sidecar_container = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=['python', '-c', XCOM_CMD],
        image='python:3.5-alpine',
        volume_mounts=[_volume_mount]
    )

    def add_sidecar(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        pod_cp = copy.deepcopy(pod)

        pod_cp.spec.volumes.insert(0, self._volume)
        pod_cp.spec.containers[0].volume_mounts.insert(0, self._volume_mount)
        pod_cp.spec.containers.append(self._sidecar_container)

        return pod_cp

    def create(self, pod: k8s.V1Pod) -> k8s.v1_pod:
        added_sidecar = self.add_sidecar(pod)
        return merge_pods(self._base_pod, added_sidecar)
