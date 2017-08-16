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

from airflow.contrib.kubernetes.kubernetes_request_factory import KubernetesRequestFactory
import logging


class KubernetesResourceBuilder:
    def __init__(
        self,
        image,
        cmds,
        args,
        namespace,
        kub_req_factory=None
    ):
        # type: (str, list, str, KubernetesRequestFactory) -> KubernetesResourceBuilder

        self.image = image
        self.args = args
        self.cmds = cmds
        self.kub_req_factory = kub_req_factory
        self.namespace = namespace
        self.logger = logging.getLogger(self.__class__.__name__)
        self.envs = {}
        self.labels = {}
        self.secrets = {}
        self.node_selectors = []
        self.name = None

    def add_env_variables(self, env):
        self.envs = env

    def add_secret(self, secret):
        self.secrets = self.secrets + [secret]

    def add_secrets(self, secrets):
        self.secrets = secrets

    def add_labels(self, labels):
        self.labels = labels

    def add_name(self, name):
        self.name = name

    def set_namespace(self, namespace):
        self.namespace = namespace


class KubernetesPodBuilder(KubernetesResourceBuilder):
    def __init__(self, image, cmds, namespace, kub_req_factory=None):
        # type: (str, list, str, KubernetesRequestFactory) -> KubernetesPodBuilder
        KubernetesResourceBuilder.__init__(self, image, cmds, namespace, kub_req_factory)
