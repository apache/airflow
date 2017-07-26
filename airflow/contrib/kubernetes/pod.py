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


class Pod(object):
    """
        Represents a kubernetes pod and manages execution of a single pod.

        :param image: The docker image
        :type image: str
        :param envs: A dict containing the environment variables
        :type envs: dict
        :param cmds: The command to be run on the pod
        :type cmds: list str
        :param secrets: Secrets to be launched to the pod
        :type secrets: list Secret
        :param result: The result that will be returned to the operator after
                       successful execution of the pod
        :type result: any

    """

    def __init__(
            self,
            image,
            envs=None,
            cmds=None,
            secrets=None,
            labels=None,
            node_selectors=None,
            name=None,
            namespace=None,
            result=None,
            configs=None,
            privileged=False,
            mount_dags=False):
        if envs is None:
            envs = {}
        self.image = image
        self.envs = envs or {}
        self.cmds = cmds or []
        self.secrets = secrets or []
        self.labels = labels or {}
        self.configs = configs or []
        self.result = result
        self.name = name
        self.node_selectors = node_selectors or []
        self.privileged = privileged
        self.mount_dags = mount_dags
        self.namespace = namespace


class Secret:
    """
        Data model for a secret

        :param deploy_type: The secret deploy type. Can be one of the following:
                            See https://kubernetes.io/docs/concepts/configuration/secret/
                            'env': to deploy secret as environment variable
                            'volume': to deploy secrets as a volume
        :type deploy_type: str
        :param deploy_target: The target of deployment. Depending on the deploy type can be
                              either an environment variable 'env' name or a volume mount
                              path 'volume'
        :type deploy_target: str
        :param secret: The secret name in Kubernetes.
        :type secret: str
        :param key: The secret key
        :type key: str
    """

    def __init__(self, deploy_type, deploy_target, secret, key):
        self.deploy_type = deploy_type
        self.deploy_target = deploy_target
        self.secret = secret
        self.key = key


class Config:
    """
        Data model for configuration data to be mounted as a file

        :param file_name: The file name for the config to be mounted
        :type file_name: str

        :param json_config: Configuration as a dict
        :type json_config: dict
    """
    def __init__(self, file_name, json_config):
        self.file_name = file_name
        self.json_config = json_config
