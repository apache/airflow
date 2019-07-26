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
import six


class KubernetesRequestFactory(metaclass=ABCMeta):
    """
    Create requests to be sent to kube API.
    Extend this class to talk to kubernetes and generate your specific resources.
    This is equivalent of generating yaml files that can be used by `kubectl`
    """

    @abstractmethod
    def create(self, pod):
        """
        Creates the request for kubernetes API.

        :param pod: The pod object
        """

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
    def add_runtime_info_env(env, runtime_info):
        env.append({
            'name': runtime_info.name,
            'valueFrom': {
                'fieldRef': {
                    'fieldPath': runtime_info.field_path
                }
            }
        })

    @staticmethod
    def extract_labels(pod, req):
        req['metadata']['labels'] = req['metadata'].get('labels', {})
        for k, v in six.iteritems(pod.labels):
            req['metadata']['labels'][k] = v

    @staticmethod
    def extract_annotations(pod, req):
        req['metadata']['annotations'] = req['metadata'].get('annotations', {})
        for k, v in six.iteritems(pod.annotations):
            req['metadata']['annotations'][k] = v

    @staticmethod
    def extract_affinity(pod, req):
        req['spec']['affinity'] = req['spec'].get('affinity', {})
        for k, v in six.iteritems(pod.affinity):
            req['spec']['affinity'][k] = v

    @staticmethod
    def extract_node_selector(pod, req):
        req['spec']['nodeSelector'] = req['spec'].get('nodeSelector', {})
        for k, v in six.iteritems(pod.node_selectors):
            req['spec']['nodeSelector'][k] = v

    @staticmethod
    def extract_cmds(pod, req):
        req['spec']['containers'][0]['command'] = pod.cmds

    @staticmethod
    def extract_args(pod, req):
        req['spec']['containers'][0]['args'] = pod.args

    @staticmethod
    def attach_ports(pod, req):
        req['spec']['containers'][0]['ports'] = (
            req['spec']['containers'][0].get('ports', []))
        if len(pod.ports) > 0:
            req['spec']['containers'][0]['ports'].extend(pod.ports)

    @staticmethod
    def attach_volumes(pod, req):
        req['spec']['volumes'] = (
            req['spec'].get('volumes', []))
        if len(pod.volumes) > 0:
            req['spec']['volumes'].extend(pod.volumes)

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
            req['spec']['containers'][0]['volumeMounts'] = (
                req['spec']['containers'][0].get('volumeMounts', []))
            req['spec']['volumes'] = (
                req['spec'].get('volumes', []))
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
        envs_from_key_secrets = [
            env for env in pod.secrets if env.deploy_type == 'env' and env.key is not None
        ]

        if len(pod.envs) > 0 or len(envs_from_key_secrets) > 0 or len(pod.pod_runtime_info_envs) > 0:
            env = []
            for k in pod.envs.keys():
                env.append({'name': k, 'value': pod.envs[k]})
            for secret in envs_from_key_secrets:
                KubernetesRequestFactory.add_secret_to_env(env, secret)
            for runtime_info in pod.pod_runtime_info_envs:
                KubernetesRequestFactory.add_runtime_info_env(env, runtime_info)

            req['spec']['containers'][0]['env'] = env

        KubernetesRequestFactory._apply_env_from(pod, req)

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
            if pod.resources.limit_memory:
                req['spec']['containers'][0]['resources']['limits'][
                    'memory'] = pod.resources.limit_memory
            if pod.resources.limit_cpu:
                req['spec']['containers'][0]['resources']['limits'][
                    'cpu'] = pod.resources.limit_cpu

    @staticmethod
    def extract_init_containers(pod, req):
        if pod.init_containers:
            req['spec']['initContainers'] = pod.init_containers

    @staticmethod
    def extract_service_account_name(pod, req):
        if pod.service_account_name:
            req['spec']['serviceAccountName'] = pod.service_account_name

    @staticmethod
    def extract_hostnetwork(pod, req):
        if pod.hostnetwork:
            req['spec']['hostNetwork'] = pod.hostnetwork

    @staticmethod
    def extract_dnspolicy(pod, req):
        if pod.dnspolicy:
            req['spec']['dnsPolicy'] = pod.dnspolicy

    @staticmethod
    def extract_image_pull_secrets(pod, req):
        if pod.image_pull_secrets:
            req['spec']['imagePullSecrets'] = [{
                'name': pull_secret
            } for pull_secret in pod.image_pull_secrets.split(',')]

    @staticmethod
    def extract_tolerations(pod, req):
        if pod.tolerations:
            req['spec']['tolerations'] = pod.tolerations

    @staticmethod
    def extract_security_context(pod, req):
        if pod.security_context:
            req['spec']['securityContext'] = pod.security_context

    @staticmethod
    def _apply_env_from(pod, req):
        envs_from_secrets = [
            env for env in pod.secrets if env.deploy_type == 'env' and env.key is None
        ]

        if pod.configmaps or envs_from_secrets:
            req['spec']['containers'][0]['envFrom'] = []

        for secret in envs_from_secrets:
            req['spec']['containers'][0]['envFrom'].append(
                {
                    'secretRef': {
                        'name': secret.secret
                    }
                }
            )

        for configmap in pod.configmaps:
            req['spec']['containers'][0]['envFrom'].append(
                {
                    'configMapRef': {
                        'name': configmap
                    }
                }
            )
