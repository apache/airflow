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

import kubernetes.client.models as k8s
from airflow.kubernetes.pod import Resources


class Pod(object):
    """
    Represents a kubernetes pod and manages execution of a single pod.
    :param image: The docker image
    :type image: str
    :param envs: A dict containing the environment variables
    :type envs: dict
    :param cmds: The command to be run on the pod
    :type cmds: list[str]
    :param secrets: Secrets to be launched to the pod
    :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
    :param result: The result that will be returned to the operator after
        successful execution of the pod
    :type result: any
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a comma separated list:
        secret_a,secret_b
    :type image_pull_secrets: str
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: dict
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: list
    :param security_context: A dict containing the security context for the pod
    :type security_context: dict
    :param configmaps: A list containing names of configmaps object
        mounting env variables to the pod
    :type configmaps: list[str]
    :param pod_runtime_info_envs: environment variables about
                                  pod runtime information (ip, namespace, nodeName, podName)
    :type pod_runtime_info_envs: list[PodRuntimeEnv]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    """
    def __init__(
            self,
            image,
            envs,
            cmds,
            args=None,
            secrets=None,
            labels=None,
            node_selectors=None,
            name=None,
            ports=None,
            volumes=None,
            volume_mounts=None,
            namespace='default',
            result=None,
            image_pull_policy='IfNotPresent',
            image_pull_secrets=None,
            init_containers=None,
            service_account_name=None,
            resources=None,
            annotations=None,
            affinity=None,
            hostnetwork=False,
            tolerations=None,
            security_context=None,
            configmaps=None,
            pod_runtime_info_envs=None,
            dnspolicy=None
    ):
        self.image = image
        self.envs = envs or {}
        self.cmds = cmds
        self.args = args or []
        self.secrets = secrets or []
        self.result = result
        self.labels = labels or {}
        self.name = name
        self.ports = ports or []
        self.volumes = volumes or []
        self.volume_mounts = volume_mounts or []
        self.node_selectors = node_selectors or {}
        self.namespace = namespace
        self.image_pull_policy = image_pull_policy
        self.image_pull_secrets = image_pull_secrets
        self.init_containers = init_containers
        self.service_account_name = service_account_name
        self.resources = resources or Resources()
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.hostnetwork = hostnetwork or False
        self.tolerations = tolerations or []
        self.security_context = security_context
        self.configmaps = configmaps or []
        self.pod_runtime_info_envs = pod_runtime_info_envs or []
        self.dnspolicy = dnspolicy

    def to_v1_kubernetes_pod(self):
        """
        Convert to support k8s V1Pod

        @return: k8s.V1Pod
        """
        meta = k8s.V1ObjectMeta(
            labels=self.labels,
            name=self.name,
            namespace=self.namespace,
        )
        spec = k8s.V1PodSpec(
            init_containers=self.init_containers,
            containers=[
                k8s.V1Container(
                    image=self.image,
                    command=self.cmds,
                    name="base",
                    env=self.envs,
                    args=self.args,
                    image_pull_policy=self.image_pull_policy,
                )
            ],
            image_pull_secrets=self.image_pull_secrets,
            service_account_name=self.service_account_name,
            dns_policy=self.dnspolicy,
            host_network=self.hostnetwork,
            tolerations=self.tolerations,
            security_context=self.security_context,
        )

        pod = k8s.V1Pod(
            spec=spec,
            metadata=meta,
        )
        for port in self.ports:
            pod = port.attach_to_pod(pod)
        for volume in self.volumes:
            pod = volume.attach_to_pod(pod)
        for volume_mount in self.volume_mounts:
            pod = volume_mount.attach_to_pod(pod)
        for secret in self.secrets:
            pod = secret.attach_to_pod(pod)
        for runtime_info in self.pod_runtime_info_envs:
            pod = runtime_info.attach_to_pod(pod)
        pod = self.resources.attach_to_pod(pod)
        return pod

    def as_dict(self):
        res = self.__dict__
        res['resources'] = res['resources'].as_dict()

        new_ports = []
        for port in res['ports']:
            new_ports.append(port.as_dict())
        res['ports'] = new_ports

        volume_mounts = []
        for volume_mount in res['volume_mounts']:
            volume_mounts.append(volume_mount.as_dict())
        res['volume_mounts'] = volume_mounts
        return res

