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
import base64
import json
import logging
import os
from abc import ABCMeta, abstractmethod
from airflow import dag_importer


class KubernetesRequestFactory(object):
    """
        Create requests to be sent to kube API. Extend this class
        to talk to kubernetes and generate your specific resources.
        This is equivalent of generating yaml files that can be used
        by `kubectl`
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def create(self, pod):
        """
            Creates the  pod request. DO NOT overwrite.
            Overwrite create_body and after_create

            :param pod: The pod object
            :returns: The Kubernetes request
            :rtype: dict
        """
        req = self.create_body(pod)
        self.after_create(req, pod)
        return req

    @abstractmethod
    def create_body(self, pod):
        """
            Creates the request body for kubernetes API.

            :param pod: The pod object
        """
        pass

    @abstractmethod
    def after_create(self, body, pod):
        """
            Is called after the create to augment the body.

            :param body: The request body
            :param pod:  The pod
        """
        pass


class KubernetesRequestFactoryHelper(object):
    """
    Helper methods to build a request for kubernetes
    """

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
        req['metadata']['labels'] = req['metadata'].get('labels') or {}
        for k in pod.labels.keys():
            req['metadata']['labels'][k] = pod.labels[k]

    @staticmethod
    def extract_cmds(pod, req):
        req['spec']['containers'][0]['command'] = pod.cmds

    @staticmethod
    def extract_node_selector(pod, req):
        req['spec']['nodeSelector'] = pod.node_selectors

    @staticmethod
    def extract_secrets(pod, req):
        env_secrets = [s for s in pod.secrets if s.deploy_type == 'env']
        if len(pod.envs) > 0 or len(env_secrets) > 0:
            env = []
            for k in pod.envs.keys():
                env.append({'name': k, 'value': pod.envs[k]})
            for secret in env_secrets:
                KubernetesRequestFactoryHelper.add_secret_to_env(env, secret)
            req['spec']['containers'][0]['env'] = env

    @staticmethod
    def attach_volume_mounts(req):
        logging.info("preparing to import dags")
        dag_importer.import_dags()
        logging.info("using file mount {}".format(dag_importer.dag_import_spec))
        container = req['spec']['containers'][0]
        container['volumeMounts'] = container.get('volumeMounts') or []
        container['volumeMounts'].append({'name': 'shared-data',
                                          'mountPath': '/usr/local/airflow/dags'})
        req['spec']['volumes'] = req['spec'].get('volumes') or []
        req['spec']['volumes'].append(dag_importer.dag_import_spec)

    @staticmethod
    def extract_name(pod, req):
        req['metadata']['name'] = KubernetesRequestFactoryHelper.sanitize_name(pod.name)
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
    def extract_injectable_configs(pod, req):
        if any(pod.configs):
            req['spec']['containers'][0]['lifecycle'] = {}
            lifecycle = req['spec']['containers'][0]['lifecycle']
            lifecycle['postStart'] = {
                'exec': {
                    'command': ['/bin/bash', '-c',
                                'echo -n $CONFIG_DATA | base64 -d > '
                                '/tmp/config_setup.sh && /bin/bash /tmp/config_setup.sh']}}
            req['spec']['containers'][0]['env'] = req['spec']['containers'][0].get('env', [])
            req['spec']['containers'][0]['env'].append({
                'name': 'CONFIG_DATA',
                'value': KubernetesRequestFactoryHelper._config_setup(pod.configs)})

    @staticmethod
    def extract_privileged(pod, req):
        if not pod.privileged:
            return
        if 'securityContext' not in req['spec']['containers']:
            req['spec']['containers'][0]['securityContext'] = {}
        req['spec']['containers'][0]['securityContext'] = {
            'privileged': True,
            'capabilities': {
                'add': [
                    'SYS_ADMIN'
                ]
            }
        }
        print('SECON', req['spec']['containers'][0]['securityContext'])

    @staticmethod
    def sanitize_name(name):
        """
        Sanitize `name` for kubernetes metadata.
        :param name: The normal name
        :return:
        """
        return name.replace('_', '-')

    @staticmethod
    def _config_setup(configs):
        dirs = ['mkdir -p "{}"'.format(os.path.split(c.file_name)[0]) for c in configs]
        config_script = "\n".join(dirs) + "\n"
        for c in configs:
            config_script += "cat<<_EOF_>{}\n".format(c.file_name)
            config_script += json.dumps(c.json_config)
            config_script += "\n_EOF_\n"
        return base64.b64encode(config_script)
