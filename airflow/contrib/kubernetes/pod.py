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


class Pod:
    """
        Represents a kubernetes pod and manages execution of a single pod.
        :param image: The docker image
        :type image: str
        :param env: A dict containing the environment variables
        :type env: dict
        :param cmds: The command to be run on the pod
        :type cmd: list str
        :param secrets: Secrets to be launched to the pod
        :type secrets: list Secret
        :param result: The result that will be returned to the operator after
                       successful execution of the pod
        :type result: any

    """
    pod_timeout = 3600

    def __init__(
            self,
            image,
            envs,
            cmds,
            secrets,
            labels=None,
            node_selectors=None,
            name=None,
            volumes = [],
            namespace='default',
            result=None,
            image_pull_policy="IfNotPresent",
            image_pull_secrets=None,
            init_containers=None,
            service_account_name=None):
        self.image = image
        self.envs = envs if envs else {}
        self.cmds = cmds
        self.secrets = secrets
        self.result = result
        self.labels = labels if labels else []
        self.name = name
        self.volumes = volumes
        self.node_selectors = node_selectors if node_selectors else []
        self.namespace = namespace
        self.image_pull_policy = image_pull_policy
        self.image_pull_secrets = image_pull_secrets
        self.init_containers = init_containers
        self.service_account_name = service_account_name
