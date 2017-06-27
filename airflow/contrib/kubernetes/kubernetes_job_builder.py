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

from kubernetes import client, config
import json
import logging


class KubernetesJobBuilder:
    def __init__(
        self,
        image,
        cmds,
        namespace,
        kub_req_factory=None
    ):
        self.image = image
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

    def add_secrets(self, secrets):
        self.secrets = secrets

    def add_labels(self, labels):
        self.labels = labels

    def add_name(self, name):
        self.name = name

    def set_namespace(self, namespace):
        self.namespace = namespace

    def launch(self):
        """
            Launches the pod synchronously and waits for completion.
        """
        k8s_beta = self._kube_client()
        req = self.kub_req_factory.create(self)
        print(json.dumps(req))
        resp = k8s_beta.create_namespaced_job(body=req, namespace=self.namespace)
        self.logger.info("Job created. status='%s', yaml:\n%s",
                         str(resp.status), str(req))

    def _kube_client(self):
        config.load_incluster_config()
        return client.BatchV1Api()

    def _execution_finished(self):
        k8s_beta = self._kube_client()
        resp = k8s_beta.read_namespaced_job_status(self.name, namespace=self.namespace)
        self.logger.info('status : ' + str(resp.status))
        if resp.status.phase == 'Failed':
            raise Exception("Job " + self.name + " failed!")
        return resp.status.phase != 'Running'
