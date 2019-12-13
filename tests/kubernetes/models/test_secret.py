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
import unittest
import unittest.mock as mock

import kubernetes.client.models as k8s
from kubernetes.client import ApiClient

from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.secret import Secret


class TestSecret(unittest.TestCase):

    def test_to_env_secret(self):
        secret = Secret('env', 'name', 'secret', 'key')
        self.assertEqual(secret.to_env_secret(), k8s.V1EnvVar(
            name='NAME',
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name='secret',
                    key='key'
                )
            )
        ))

    def test_to_env_from_secret(self):
        secret = Secret('env', None, 'secret')
        self.assertEqual(secret.to_env_from_secret(), k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name='secret')
        ))

    @mock.patch('uuid.uuid4')
    def test_to_volume_secret(self, mock_uuid):
        mock_uuid.return_value = '0'
        secret = Secret('volume', '/etc/foo', 'secret_b')
        self.assertEqual(secret.to_volume_secret(), (
            k8s.V1Volume(
                name='secretvol0',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='secret_b'
                )
            ),
            k8s.V1VolumeMount(
                mount_path='/etc/foo',
                name='secretvol0',
                read_only=True
            )
        ))

    @mock.patch('uuid.uuid4')
    def test_attach_to_pod(self, mock_uuid):
        mock_uuid.return_value = '0'
        pod = PodGenerator(image='airflow-worker:latest',
                           name='base').gen_pod()
        secrets = [
            # This should be a secretRef
            Secret('env', None, 'secret_a'),
            # This should be a single secret mounted in volumeMounts
            Secret('volume', '/etc/foo', 'secret_b'),
            # This should produce a single secret mounted in env
            Secret('env', 'TARGET', 'secret_b', 'source_b'),
        ]
        k8s_client = ApiClient()
        result = append_to_pod(pod, secrets)
        result = k8s_client.sanitize_for_serialization(result)
        self.assertEqual(result, {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'base-0'},
            'spec': {
                'containers': [{
                    'args': [],
                    'command': [],
                    'env': [{
                        'name': 'TARGET',
                        'valueFrom': {
                            'secretKeyRef': {
                                'key': 'source_b',
                                'name': 'secret_b'
                            }
                        }
                    }],
                    'envFrom': [{'secretRef': {'name': 'secret_a'}}],
                    'image': 'airflow-worker:latest',
                    'imagePullPolicy': 'IfNotPresent',
                    'name': 'base',
                    'ports': [],
                    'volumeMounts': [{
                        'mountPath': '/etc/foo',
                        'name': 'secretvol0',
                        'readOnly': True}]
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'restartPolicy': 'Never',
                'volumes': [{
                    'name': 'secretvol0',
                    'secret': {'secretName': 'secret_b'}
                }]
            }
        })
