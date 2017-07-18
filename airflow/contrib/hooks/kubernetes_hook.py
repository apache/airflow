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

import logging
import requests
import json

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from kubernetes import client, config

class KubernetesHook(BaseHook):
    """
    Kubernetes interaction hook

    :param k8s_conn_id: reference to a pre-defined K8s Connection
    :type k8s_conn_id: string
    """

    def __init__(self, k8s_conn_id="k8s_default"):
        self.conn_id = k8s_conn_id
        self.core_client = None

    def get_conn(self):
        """
        Initializes the api client. Only config file or env
        configuration supported at the moment.
        """
        if not self.core_client:
            config.load_kube_config()
            self.core_client = client.CoreV1Api()

        return self.core_client

    def get_env_definitions(self, env):
        def get_env(name, definition):
            if isinstance(definition, str):
                return client.V1EnvVar(name=name, value=definition)
            elif isinstance(definition, dict):
                source = definition['source']
                if source == 'configMap':
                    return client.V1EnvVar(name=name,
                            value_from=client.V1EnvVarSource(
                                config_map_key_ref=client.V1ConfigMapKeySelector(
                                    key=definition['key'], name=definition['name'])))
                elif source == 'secret':
                    return client.V1EnvVar(name=name,
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    key=definition['key'], name=definition['name'])))
                else:
                    raise AirflowException('Creating env vars from %s not implemented',
                            source)
            else:
                raise AirflowException('Environment variable definition \
                        has to be either string or a dictionary. %s given instead',
                        type(definition))

        return [get_env(name, definition) for name, definition in env.items()]

    def get_env_from_definitions(self, env_from):
        def get_env_from(definition):
            configmap = definition.get('configMap')
            secret = definition.get('secret')
            prefix = definition.get('prefix')

            cfg_ref = client.V1ConfigMapEnvSource(name=configmap) if configmap else None
            secret_ref = client.V1SecretEnvSource(name=secret) if secret else None

            return client.V1EnvFromSource(
                config_map_ref=cfg_ref,
                secret_ref=secret_ref,
                prefix=prefix
            )
        return [get_env_from(definition) for definition in env_from]

    def get_volume_definitions(self, volumes):
        def get_volume(name, definition):
            if definition['type'] == 'emptyDir':
                volume = client.V1Volume(
                    name=name,
                    empty_dir=client.V1EmptyDirVolumeSource()
                )
                volume_mount = client.V1VolumeMount(
                    mount_path=definition['mountPath'],
                    name=name
                )
            elif definition['type'] == 'hostPath':
                volume = client.V1Volume(
                    name=name,
                    host_path=client.V1HostPathVolumeSource(
                        path=definition['path']
                    )
                )
                volume_mount = client.V1VolumeMount(
                    mount_path=definition['mountPath'],
                    name=name
                )
            elif definition['type'] == 'secret':
                volume = client.V1Volume(
                    name=name,
                    secret=client.V1SecretVolumeSource(
                        secret_name=definition['secret']
                    )
                )
                volume_mount = client.V1VolumeMount(
                    mount_path=definition['mountPath'],
                    name=name
                )
            else:
               raise AirflowException('Volume source %s not implemented',
                    definition['type'])

            return (volume, volume_mount)

        [volume_defs, volume_mount_defs] = \
                zip(*[get_volume(name, definition) for name, definition in volumes.items()])
        return (list(volume_defs), list(volume_mount_defs))

    def get_pod_definition(
            self,
            image,
            name,
            namespace=None,
            restart_policy="Never",
            command=None,
            args=None,
            env=None,
            env_from=None,
            volumes=None,
            labels=None):
        """
            Builds pod definition based on supplied arguments
        """
        env_defs = self.get_env_definitions(env) if env else None
        env_from_defs = self.get_env_from_definitions(env_from) if env_from else None
        volume_defs, volume_mount_defs = \
                self.get_volume_definitions(volumes) if volumes else (None, None)

        return client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=namespace,
                labels=labels
            ),
            spec=client.V1PodSpec(
                restart_policy=restart_policy,
                containers=[client.V1Container(
                    name=name,
                    command=command,
                    args=args,
                    image=image,
                    env=env_defs,
                    env_from=env_from_defs,
                    volume_mounts=volume_mount_defs
                )],
                volumes=volume_defs
            )
        )

    def create_pod(self, pod):
        namespace = pod.metadata.namespace
        self.get_conn().create_namespaced_pod(namespace, pod)

    def delete_pod(
            self,
            pod=None,
            name=None,
            namespace=None):
        """
            Delete a pod based on pod definition or name
        """
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace
        self.get_conn().delete_namespaced_pod(name, namespace, client.V1DeleteOptions())

    def get_pod_state(self, pod=None, name=None, namespace=None):
        """
            Fetches pod status and returns phase
        """
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace

        pod_status = self.get_conn().read_namespaced_pod_status(name, namespace)

        if not pod_status:
            raise AirflowException("Cannot find the requested pod!")

        return pod_status.status.phase

    def relay_pod_logs(self, pod=None, name=None, namespace=None):
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace

        logging.info("Start container log")
        logging.info("-------------------")

        if not self._stream_log(name, namespace):
            self._client_log(name, namespace)

    def _get_authorization(self):
        if client.configuration.api_key['authorization'] is not None:
            return {'Authorization': client.configuration.api_key['authorization']}
        else:
            return None

    def _stream_log(self, name, namespace):
        """
            Stream logs for pod.
            The python-client for kubernetes does not (yet) support iterating over a
            streaming log.

            Only bearer authenticated requests for now.
            (Which is enough if running the worker in kubernetes)
        """
        headers = self._get_authorization()
        if not headers:
            return False

        try:
            url = "%s/api/v1/namespaces/%s/pods/%s/log" % \
                    (client.configuration.host, namespace, name)
            with requests.get(url,
                             params={'follow':'true'},
                             verify=client.configuration.ssl_ca_cert,
                             headers=headers,
                             stream=True) as r:

                if r.encoding is None:
                    r.encoding = 'utf-8'

                for line in r.iter_lines(decode_unicode=True):
                    logging.info(line.strip())
        except Exception as e:
            logging.info("Streaming container log terminated unexpectedly: %s", e)
            return False

        return True

    def _client_log(self, name, namespace):
        """
            Fetch log from k8s client.
            read_namespaced_pod_log with follow=True, only returns once the log is
            closed.
        """
        try:
            log = self.get_conn().read_namespaced_pod_log(
                    name,
                    namespace,
                    follow=True)

            log_lines = log.rstrip().split("\n")
            for line in log_lines:
                logging.info(line.rstrip())
        except Exception as e:
            logging.info("Container log from client terminated unexpectedly: %s", e)

    def relay_pod_events(self, pod=None, name=None, namespace=None, timeout=60):
        """
            Stream kubernetes events for the pod into logging.info

            Watches the events for the specified pod, until either an event with
            reason "Started" is encountered, or a timeout is reached. Some events
            might be missed as the api does not necessarily return
            events in order, however this should not be a real problem as the value
            of these are diagnosing startup problems.
        """
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace

        headers = self._get_authorization()
        if not headers:
            return False

        params = {
            'fieldSelector': 'involvedObject.name=%s' % name,
            'watch': 'true',
            'timeoutSeconds': timeout
        }

        url = "%s/api/v1/namespaces/%s/events/" % (
                client.configuration.host,
                namespace)

        logging.info("Start pod event log")

        try:
            with requests.get(url,
                             params=params,
                             verify=client.configuration.ssl_ca_cert,
                             headers=headers,
                             stream=True) as r:

                if r.encoding is None:
                    r.encoding = 'utf-8'

                for line in r.iter_lines(decode_unicode=True):
                    data = json.loads(line)

                    ob = data['object']

                    logging.info("event: %s (component: %s, host: %s, reason: %s)",
                            ob.get('message'),
                            ob.get('source', {}).get('component'),
                            ob.get('source', {}).get('host'),
                            ob.get('reason'),
                    )

                    if ob.get('reason') == "Started":
                        break

        except Exception as e:
            logging.info("Streaming events terminated unexpectedly: %s", e)
            return False

        return True
