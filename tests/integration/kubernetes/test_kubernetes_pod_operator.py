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

import json
import os
import shutil
import unittest
from subprocess import check_call
from unittest import mock
from unittest.mock import ANY

import kubernetes.client.models as k8s
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow import AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.version import version as airflow_version

try:
    check_call(["/usr/local/bin/kubectl", "get", "pods"])
except Exception as e:  # pylint: disable=broad-except
    if os.environ.get('KUBERNETES_VERSION') and os.environ.get('ENV', 'kubernetes') == 'kubernetes':
        raise e
    else:
        raise unittest.SkipTest(
            "Kubernetes integration tests require a kubernetes cluster;"
            "Skipping tests {}".format(e)
        )


# pylint: disable=unused-argument
class TestKubernetesPodOperator(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None  # pylint: disable=invalid-name
        self.api_client = ApiClient()
        self.expected_pod = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'namespace': 'default',
                'name': ANY,
                'annotations': {},
                'labels': {
                    'foo': 'bar', 'kubernetes_pod_operator': 'True',
                    'airflow_version': airflow_version.replace('+', '-')
                }
            },
            'spec': {
                'affinity': {},
                'containers': [{
                    'image': 'ubuntu:16.04',
                    'args': ["echo 10"],
                    'command': ["bash", "-cx"],
                    'env': [],
                    'imagePullPolicy': 'IfNotPresent',
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'resources': {'limits': {'cpu': None,
                                             'memory': None,
                                             'nvidia.com/gpu': None},
                                  'requests': {'cpu': None,
                                               'memory': None}},
                    'volumeMounts': [],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'initContainers': [],
                'nodeSelector': {},
                'restartPolicy': 'Never',
                'securityContext': {},
                'serviceAccountName': 'default',
                'tolerations': [],
                'volumes': [],
            }
        }

    def test_config_path_move(self):
        new_config_path = '/tmp/kube_config'
        old_config_path = os.path.expanduser('~/.kube/config')
        shutil.copy(old_config_path, new_config_path)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            config_file=new_config_path,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_config_path(self, client_mock, launcher_mock):
        from airflow.utils.state import State

        file_path = "/tmp/fake_file"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            config_file=file_path,
            cluster_context='default',
        )
        launcher_mock.return_value = (State.SUCCESS, None)
        k.execute(None)
        client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context='default',
            config_file=file_path,
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, launcher_mock):
        from airflow.utils.state import State

        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_secrets=fake_pull_secrets,
            cluster_context='default',
        )
        launcher_mock.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(
            launcher_mock.call_args[0][0].spec.image_pull_secrets,
            [k8s.V1LocalObjectReference(name=fake_pull_secrets)]
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.delete_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_delete_even_on_launcher_error(self, mock_client, delete_pod_mock, run_pod_mock):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            is_delete_operator_pod=True,
        )
        run_pod_mock.side_effect = AirflowException('fake failure')
        with self.assertRaises(AirflowException):
            k.execute(None)
        assert delete_pod_mock.called

    def test_working_pod(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod, actual_pod)

    def test_delete_operator_pod(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=True,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_hostnetwork(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_dnspolicy(self):
        dns_policy = "ClusterFirstWithHostNet"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
            dnspolicy=dns_policy
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        self.expected_pod['spec']['dnsPolicy'] = dns_policy
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_node_selectors(self):
        node_selectors = {
            'beta.kubernetes.io/os': 'linux'
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            node_selectors=node_selectors,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['nodeSelector'] = node_selectors
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_resources(self):
        resources = {
            'limit_cpu': 0.25,
            'limit_memory': '64Mi',
            'request_cpu': '250m',
            'request_memory': '64Mi',
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            resources=resources,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['resources'] = {
            'requests': {
                'memory': '64Mi',
                'cpu': '250m'
            },
            'limits': {
                'memory': '64Mi',
                'cpu': 0.25,
                'nvidia.com/gpu': None
            }
        }
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_affinity(self):
        affinity = {
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [
                        {
                            'matchExpressions': [
                                {
                                    'key': 'beta.kubernetes.io/os',
                                    'operator': 'In',
                                    'values': ['linux']
                                }
                            ]
                        }
                    ]
                }
            }
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            affinity=affinity,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['affinity'] = affinity
        self.assertEqual(self.expected_pod, actual_pod)

    def test_port(self):
        port = Port('http', 80)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            ports=[port],
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['ports'] = [{
            'name': 'http',
            'containerPort': 80
        }]
        self.assertEqual(self.expected_pod, actual_pod)

    def test_volume_mount(self):
        with mock.patch.object(PodLauncher, 'log') as mock_logger:
            volume_mount = VolumeMount('test-volume',
                                       mount_path='/root/mount_file',
                                       sub_path=None,
                                       read_only=True)

            volume_config = {
                'persistentVolumeClaim':
                    {
                        'claimName': 'test-volume'
                    }
            }
            volume = Volume(name='test-volume', configs=volume_config)
            args = ["cat /root/mount_file/test.txt"]
            k = KubernetesPodOperator(
                namespace='default',
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=args,
                labels={"foo": "bar"},
                volume_mounts=[volume_mount],
                volumes=[volume],
                name="test",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
            )
            k.execute(None)
            mock_logger.info.assert_any_call(b"retrieved from mount\n")
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['args'] = args
            self.expected_pod['spec']['containers'][0]['volumeMounts'] = [{
                'name': 'test-volume',
                'mountPath': '/root/mount_file',
                'readOnly': True
            }]
            self.expected_pod['spec']['volumes'] = [{
                'name': 'test-volume',
                'persistentVolumeClaim': {
                    'claimName': 'test-volume'
                }
            }]
            self.assertEqual(self.expected_pod, actual_pod)

    def test_run_as_user_root(self):
        security_context = {
            'securityContext': {
                'runAsUser': 0,
            }
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        self.assertEqual(self.expected_pod, actual_pod)

    def test_run_as_user_non_root(self):
        security_context = {
            'securityContext': {
                'runAsUser': 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        self.assertEqual(self.expected_pod, actual_pod)

    def test_fs_group(self):
        security_context = {
            'securityContext': {
                'fsGroup': 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        self.assertEqual(self.expected_pod, actual_pod)

    def test_faulty_image(self):
        bad_image_name = "foobar"
        k = KubernetesPodOperator(
            namespace='default',
            image=bad_image_name,
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
        )
        with self.assertRaises(AirflowException):
            k.execute(None)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['image'] = bad_image_name
            self.assertEqual(self.expected_pod, actual_pod)

    def test_faulty_service_account(self):
        bad_service_account_name = "foobar"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
            service_account_name=bad_service_account_name,
        )
        with self.assertRaises(ApiException):
            k.execute(None)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['serviceAccountName'] = bad_service_account_name
            self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_failure(self):
        """
            Tests that the task fails when a pod reports a failure
        """
        bad_internal_command = ["foobar 10 "]
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=bad_internal_command,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        with self.assertRaises(AirflowException):
            k.execute(None)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['args'] = bad_internal_command
            self.assertEqual(self.expected_pod, actual_pod)

    def test_xcom_push(self):
        return_value = '{"foo": "bar"\n, "buzz": 2}'
        args = ['echo \'{}\' > /airflow/xcom/return.json'.format(return_value)]
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=args,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=True,
        )
        self.assertEqual(k.execute(None), json.loads(return_value))
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(PodDefaults.SIDECAR_CONTAINER)
        self.expected_pod['spec']['containers'][0]['args'] = args
        self.expected_pod['spec']['containers'][0]['volumeMounts'].insert(0, volume_mount)
        self.expected_pod['spec']['volumes'].insert(0, volume)
        self.expected_pod['spec']['containers'].append(container)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_launcher):
        # GIVEN
        from airflow.utils.state import State

        configmap = 'test-configmap'
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            configmaps=[configmap],
        )
        # THEN
        mock_launcher.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(
            mock_launcher.call_args[0][0].spec.containers[0].env_from,
            [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(
                name=configmap
            ))]
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_secrets(self, mock_client, launcher_mock):
        # GIVEN
        from airflow.utils.state import State
        secret_ref = 'secret_name'
        secrets = [Secret('env', None, secret_ref)]
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            secrets=secrets,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        # THEN
        launcher_mock.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(
            launcher_mock.call_args[0][0].spec.containers[0].env_from,
            [k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(
                name=secret_ref
            ))]
        )

    def test_init_container(self):
        # GIVEN
        volume_mounts = [k8s.V1VolumeMount(
            mount_path='/etc/foo',
            name='test-volume',
            sub_path=None,
            read_only=True
        )]

        init_environments = [k8s.V1EnvVar(
            name='key1',
            value='value1'
        ), k8s.V1EnvVar(
            name='key2',
            value='value2'
        )]

        init_container = k8s.V1Container(
            name="init-container",
            image="ubuntu:16.04",
            env=init_environments,
            volume_mounts=volume_mounts,
            command=["bash", "-cx"],
            args=["echo 10"]
        )

        volume_config = {
            'persistentVolumeClaim':
            {
                'claimName': 'test-volume'
            }
        }
        volume = Volume(name='test-volume', configs=volume_config)

        expected_init_container = {
            'name': 'init-container',
            'image': 'ubuntu:16.04',
            'command': ['bash', '-cx'],
            'args': ['echo 10'],
            'env': [{
                'name': 'key1',
                'value': 'value1'
            }, {
                'name': 'key2',
                'value': 'value2'
            }],
            'volumeMounts': [{
                'mountPath': '/etc/foo',
                'name': 'test-volume',
                'readOnly': True
            }],
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            volumes=[volume],
            init_containers=[init_container],
            in_cluster=False,
            do_xcom_push=False,
        )

        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['initContainers'] = [expected_init_container]
        self.expected_pod['spec']['volumes'] = [{
            'name': 'test-volume',
            'persistentVolumeClaim': {
                'claimName': 'test-volume'
            }
        }]
        self.assertEqual(self.expected_pod, actual_pod)


# pylint: enable=unused-argument
if __name__ == '__main__':
    unittest.main()
