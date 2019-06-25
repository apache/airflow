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


class Resources:
    def __init__(
            self,
            request_memory=None,
            request_cpu=None,
            limit_memory=None,
            limit_cpu=None):
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu

    def is_empty_resource_request(self):
        return not self.has_limits() and not self.has_requests()

    def has_limits(self):
        return self.limit_cpu is not None or self.limit_memory is not None

    def has_requests(self):
        return self.request_cpu is not None or self.request_memory is not None


class Port:
    def __init__(
            self,
            name=None,
            container_port=None):
        self.name = name
        self.container_port = container_port


class Pod:
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
