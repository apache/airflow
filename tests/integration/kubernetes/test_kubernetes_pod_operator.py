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
from textwrap import dedent
from unittest import mock
from unittest.mock import ANY

import kubernetes.client.models as k8s
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow import AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator, KubernetesPodYamlOperator
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.pod_enricher import _AIRFLOW_VERSION, XcomSidecarConfig
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.utils.state import State

try:
    check_call(["/usr/local/bin/kubectl", "get", "pods"])
except Exception as e:  # pylint: disable=broad-except
    if os.environ.get('KUBERNETES_VERSION'):
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
                'labels': {'foo': 'bar', 'airflow-version': _AIRFLOW_VERSION}
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
                    'volumeMounts': [],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
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
            config_file=new_config_path
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_config_path(self, client_mock, launcher_mock):
        file_path = "/tmp/fake_file"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            config_file=file_path,
            in_cluster=False,
            cluster_context='default'
        )
        launcher_mock.return_value = (State.SUCCESS, None)
        k.execute(None)
        client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context='default',
            config_file=file_path
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, launcher_mock):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            image_pull_secrets=fake_pull_secrets,
            in_cluster=False,
            cluster_context='default'
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
            cluster_context='default',
            is_delete_operator_pod=True
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
            task_id="task"
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
            is_delete_operator_pod=True
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
            hostnetwork=True
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
            node_selectors=node_selectors,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['nodeSelector'] = node_selectors
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_resources(self):
        resources = {
            'limits': {
                'cpu': '250m',
                'memory': '64Mi',
            },
            'requests': {
                'cpu': '250m',
                'memory': '64Mi',
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
            resources=resources,
        )
        k.execute(None)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['resources'] = resources
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
            ports=[port]
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
                task_id="task"
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
            startup_timeout_seconds=5
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
            startup_timeout_seconds=5,
            service_account_name=bad_service_account_name
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
            task_id="task"
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
            do_xcom_push=True
        )
        self.assertEqual(k.execute(None), json.loads(return_value))
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(XcomSidecarConfig.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(XcomSidecarConfig.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(XcomSidecarConfig.CONTAINER)
        self.expected_pod['spec']['containers'][0]['args'] = args
        self.expected_pod['spec']['containers'][0]['volumeMounts'].insert(0, volume_mount)
        self.expected_pod['spec']['volumes'].insert(0, volume)
        self.expected_pod['spec']['containers'].append(container)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_launcher):
        # GIVEN
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
            configmaps=[configmap]
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


class TestKubernetesPodYamlOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_launcher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_enricher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.k8s_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_success_launch_pod(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
        mock_k8s_deserializer,
        mock_pod_enricher,
        mock_pod_launcher
    ):
        yaml_file = [
            {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': 'myapp-pod',
                },
                'spec': {
                    'containers': [
                        {
                            'name': 'base',
                            'image': 'busybox',
                        }
                    ],
                }
            }
        ]
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        mock_pod_launcher.PodLauncher.return_value.run_pod.return_value = (State.SUCCESS, "RESULT")
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=False,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        task.execute(mock.MagicMock())
        mock_kube_client.get_kube_client.assert_called_once_with(
            cluster_context="CLUSTER_CONTEXT", config_file="CONFIG_FILE", in_cluster="IN_CLUSTER"
        )
        mock_yaml_deserializer.safe_load_all.assert_called_once_with('yaml-file.yaml')
        mock_pod_enricher.refine_pod.assert_called_once_with(
            mock_k8s_deserializer.deserialize.return_value, extract_xcom="DO_XCOM_PUSH",
        )
        mock_k8s_deserializer.deserialize.assert_called_once_with(
            yaml_file[0], k8s.V1Pod
        )
        mock_pod_launcher.PodLauncher.assert_called_once_with(
            extract_xcom='DO_XCOM_PUSH',
            kube_client=mock_kube_client.get_kube_client.return_value
        )
        mock_pod_launcher.PodLauncher.return_value.run_pod.assert_called_once_with(
            get_logs=True, pod=mock_pod_enricher.refine_pod.return_value, startup_timeout=120
        )

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_launcher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_enricher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.k8s_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_launch_pod_with_delete(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
        mock_k8s_deserializer,
        mock_pod_enricher,
        mock_pod_launcher
    ):
        yaml_file = [
            {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': 'myapp-pod',
                },
                'spec': {
                    'containers': [
                        {
                            'name': 'base',
                            'image': 'busybox',
                        }
                    ],
                }
            }
        ]
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        mock_pod_launcher.PodLauncher.return_value.run_pod.return_value = (State.SUCCESS, "RESULT")
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=True,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        task.execute(mock.MagicMock())
        mock_kube_client.get_kube_client.assert_called_once_with(
            cluster_context="CLUSTER_CONTEXT", config_file="CONFIG_FILE", in_cluster="IN_CLUSTER"
        )
        mock_yaml_deserializer.safe_load_all.assert_called_once_with('yaml-file.yaml')
        mock_pod_enricher.refine_pod.assert_called_once_with(
            mock_k8s_deserializer.deserialize.return_value, extract_xcom="DO_XCOM_PUSH",
        )
        mock_k8s_deserializer.deserialize.assert_called_once_with(
            yaml_file[0], k8s.V1Pod
        )
        mock_pod_launcher.PodLauncher.assert_called_once_with(
            extract_xcom='DO_XCOM_PUSH',
            kube_client=mock_kube_client.get_kube_client.return_value
        )
        mock_pod_launcher.PodLauncher.return_value.run_pod.assert_called_once_with(
            get_logs=True, pod=mock_pod_enricher.refine_pod.return_value, startup_timeout=120
        )
        mock_pod_launcher.PodLauncher.return_value.delete_pod.assert_called_once_with(
            mock_pod_enricher.refine_pod.return_value
        )

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_missing_pod_defintion(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
    ):
        yaml_file = []
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=False,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        with self.assertRaisesRegex(
            AirflowException, "Pod Launching failed: You must specify Pod resource definitions."
        ):
            task.execute(mock.MagicMock())

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_multiple_pod_definition(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
    ):
        yaml_file = [{}, {}]
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=False,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        with self.assertRaisesRegex(
            AirflowException,
            "Pod Launching failed: You can only run one Pod at a time. Please delete the other "
            "resource definitions from the YAML file"
        ):
            task.execute(mock.MagicMock())

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_launcher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_enricher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.k8s_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_pod_return_failure(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
        mock_k8s_deserializer,
        mock_pod_enricher,
        mock_pod_launcher
    ):
        yaml_file = [
            {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': 'myapp-pod',
                },
                'spec': {
                    'containers': [
                        {
                            'name': 'base',
                            'image': 'busybox',
                        }
                    ],
                }
            }
        ]
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        mock_pod_launcher.PodLauncher.return_value.run_pod.return_value = (State.FAILED, "RESULT")
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=False,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        with self.assertRaisesRegex(AirflowException, "Pod Launching failed: Pod returned a failure: failed"):
            task.execute(mock.MagicMock())

    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_launcher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.pod_enricher')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.k8s_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.yaml_deserializer')
    @mock.patch('airflow.contrib.operators.kubernetes_pod_operator.kube_client')
    def test_failed_launch_pod(
        self,
        mock_kube_client,
        mock_yaml_deserializer,
        mock_k8s_deserializer,
        mock_pod_enricher,
        mock_pod_launcher
    ):
        yaml_file = [
            {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': 'myapp-pod',
                },
                'spec': {
                    'containers': [
                        {
                            'name': 'base',
                            'image': 'busybox',
                        }
                    ],
                }
            }
        ]
        ex = AirflowException("TEST EXCEPTION")
        mock_yaml_deserializer.safe_load_all.return_value = yaml_file
        mock_pod_launcher.PodLauncher.return_value.run_pod.side_effect = ex
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml="yaml-file.yaml",
            do_xcom_push="DO_XCOM_PUSH",
            config_file="CONFIG_FILE",
            in_cluster="IN_CLUSTER",
            cluster_context="CLUSTER_CONTEXT",
            is_delete_operator_pod=False,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        with self.assertRaisesRegex(AirflowException, "Pod Launching failed: TEST EXCEPTION"):
            task.execute(mock.MagicMock())


class TestIntegrationKubernetesPodYamlOperator(unittest.TestCase):

    def test_start_pod(self):
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml=dedent("""
                apiVersion: v1
                kind: Pod
                metadata:
                  labels:
                    run: example-yaml-2
                  name: example-yaml-2
                spec:
                  containers:
                  - args:
                    - sh
                    - -c
                    - echo 123; sleep 10
                    image: busybox
                    name: example-yaml-2
                  restartPolicy: Never
            """),
            do_xcom_push=False,
            is_delete_operator_pod=True,
        )
        result = task.execute({})
        self.assertEqual(None, result)

    def test_start_pod_with_xcom(self):
        task = KubernetesPodYamlOperator(
            task_id='pod-yaml',
            yaml=dedent("""
                apiVersion: v1
                kind: Pod
                metadata:
                  name: example-yaml-xcom
                spec:
                  containers:
                  - args:
                    - sh
                    - -c
                    - mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json
                    image: alpine
                    name: example-yaml-xcom
                  restartPolicy: Never
            """),
            do_xcom_push=True,
        )
        result = task.execute({})
        self.assertEqual([1, 2, 3, 4], result)


# pylint: enable=unused-argument
if __name__ == '__main__':
    unittest.main()
