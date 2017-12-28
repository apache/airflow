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
    def attach_volume_mounts(pod, req):
        if len(pod.volume_mounts) > 0:
            req['spec']['containers'][0]['volumeMounts'] = (
                req['spec']['containers'][0].get('volumeMounts', []))
            req['spec']['containers'][0]['volumeMounts'].extend(pod.volume_mounts)

    @staticmethod
    def extract_name(pod, req):
        req['metadata']['name'] = pod.name

    @staticmethod
    def extract_volume_secrets(pod, req):
        vol_secrets = [s for s in pod.secrets if s.deploy_type == 'volume']
        if any(vol_secrets):
            req['spec']['containers'][0]['volumeMounts'] = []
            req['spec']['volumes'] = []
        for idx, vol in enumerate(vol_secrets):
            vol_id = 'secretvol' + str(idx)
            req['spec']['containers'][0]['volumeMounts'].append({
                'mountPath': vol.deploy_target,
                'name': vol_id,
                'readOnly': True
            })
            req['spec']['volumes'].append({
                'name': vol_id,
                'secret': {
                    'secretName': vol.secret
                }
            })

    @staticmethod
    def extract_env_and_secrets(pod, req):
        env_secrets = [s for s in pod.secrets if s.deploy_type == 'env']
        if len(pod.envs) > 0 or len(env_secrets) > 0:
            env = []
            for k in pod.envs.keys():
                env.append({'name': k, 'value': pod.envs[k]})
            for secret in env_secrets:
                KubernetesRequestFactory.add_secret_to_env(env, secret)
            req['spec']['containers'][0]['env'] = env



    @staticmethod
    def extract_init_containers(pod, req):
        if pod.init_containers:
            req['spec']['initContainers'] = pod.init_containers

    @staticmethod
    def extract_service_account_name(pod, req):
        if pod.service_account_name:
            req['spec']['serviceAccountName'] = pod.service_account_name

    @staticmethod
    def extract_image_pull_secrets(pod, req):
        if pod.image_pull_secrets:
            req['spec']['imagePullSecrets'] = [{
                'name': pull_secret
            } for pull_secret in pod.image_pull_secrets.split(',')]
