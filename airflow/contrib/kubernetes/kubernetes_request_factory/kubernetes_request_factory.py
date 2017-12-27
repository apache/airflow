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

from abc import ABCMeta, abstractmethod
import six


class KubernetesRequestFactory:
    """
    Create requests to be sent to kube API.
    Extend this class to talk to kubernetes and generate your specific resources.
    This is equivalent of generating yaml files that can be used by `kubectl`
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def create(self, pod):
        """
        Creates the request for kubernetes API.

        :param pod: The pod object
        """
        pass

    @staticmethod
    def extract_image(pod, req):
        req['spec']['containers'][0]['image'] = pod.image

    @staticmethod
    def extract_image_pull_policy(pod, req):
        if pod.image_pull_policy:
            req['spec']['containers'][0]['imagePullPolicy'] = pod.image_pull_policy

    @staticmethod
    def add_secret_to_env(env, secret):
        env.append({
            'name': secret.deploy_target,
            'valueFrom': {
                'secretKeyRef': {
                    'name': secret.secret,
                    'key': secret.key
                }
            }
        })

    @staticmethod
    def extract_labels(pod, req):
        req['metadata']['labels'] = req['metadata'].get('labels', {})
        for k, v in six.iteritems(pod.labels):
            req['metadata']['labels'][k] = v

    @staticmethod
    def extract_cmds(pod, req):
        req['spec']['containers'][0]['command'] = pod.cmds

    @staticmethod
    def extract_args(pod, req):
        req['spec']['containers'][0]['args'] = pod.args

    @staticmethod
    def extract_node_selector(pod, req):
        if len(pod.node_selectors) > 0:
            req['spec']['nodeSelector'] = pod.node_selectors

    @staticmethod
    def attach_volumes(pod, req):
        req['spec']['volumes'] = pod.volumes

    @staticmethod
    def attach_volume_mounts(pod, req):
        if len(pod.volume_mounts) > 0:
            req['spec']['containers'][0]['volumeMounts'] = (
                req['spec']['containers'][0].get('volumeMounts', []))
            req['spec']['containers'][0]['volumeMounts'].extend(pod.volume_mounts)

    @staticmethod
    def extract_name(pod, req):
        req['metadata']['name'] = pod.name

    @staticmethod
    def extract_resources(pod, req):
        if not pod.resources or pod.resources.is_empty_resource_request():
            return

        req['spec']['containers'][0]['resources'] = {}

        if pod.resources.has_requests():
            req['spec']['containers'][0]['resources']['requests'] = {}
            if pod.resources.request_memory:
                req['spec']['containers'][0]['resources']['requests'][
                    'memory'] = pod.resources.request_memory
            if pod.resources.request_cpu:
                req['spec']['containers'][0]['resources']['requests'][
                    'cpu'] = pod.resources.request_cpu

        if pod.resources.has_limits():
            req['spec']['containers'][0]['resources']['limits'] = {}
            if pod.resources.request_memory:
                req['spec']['containers'][0]['resources']['limits'][
                    'memory'] = pod.resources.limit_memory
            if pod.resources.request_cpu:
                req['spec']['containers'][0]['resources']['limits'][
                    'cpu'] = pod.resources.limit_cpu
