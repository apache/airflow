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
from unittest import mock
from unittest.mock import ANY

import kubernetes.client.models as k8s
import pendulum
import pytest
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils import timezone
from airflow.version import version as airflow_version


@pytest.mark.runtime("kubernetes")
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
                    'airflow_version': airflow_version.replace('+', '-'),
                    'execution_date': '2016-01-01T0100000100-a2f50a31f',
                    'dag_id': 'dag',
                    'task_id': 'task',
                    'try_number': '1'},
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
                'initContainers': [],
                'nodeSelector': {},
                'restartPolicy': 'Never',
                'securityContext': {},
                'serviceAccountName': 'default',
                'tolerations': [],
                'volumes': [],
            }
        }

    def tearDown(self) -> None:
        client = kube_client.get_kube_client(in_cluster=False)
        client.delete_collection_namespaced_pod(namespace="default")

    def create_context(self, task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task,
                                     execution_date=execution_date)
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    def test_do_xcom_push_defaults_false(self):
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
        self.assertFalse(k.do_xcom_push)

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
            name="test1",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            config_file=new_config_path,
        )
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_config_path(self, client_mock, monitor_mock, start_mock):  # pylint: disable=unused-argument
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
        monitor_mock.return_value = (State.SUCCESS, None)
        client_mock.list_namespaced_pod.return_value = []
        context = self.create_context(k)
        k.execute(context=context)
        client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context='default',
            config_file=file_path,
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, monitor_mock, start_mock):
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
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context=context)
        self.assertEqual(
            start_mock.call_args[0][0].spec.image_pull_secrets,
            [k8s.V1LocalObjectReference(name=fake_pull_secrets)]
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.delete_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_delete_even_on_launcher_error(
            self,
            mock_client,
            delete_pod_mock,
            monitor_pod_mock,
            start_pod_mock):  # pylint: disable=unused-argument
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
        monitor_pod_mock.side_effect = AirflowException('fake failure')
        with self.assertRaises(AirflowException):
            context = self.create_context(k)
            k.execute(context=context)
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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod['spec'], actual_pod['spec'])
        self.assertEqual(self.expected_pod['metadata']['labels'], actual_pod['metadata']['labels'])

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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual(self.expected_pod['spec'], actual_pod['spec'])
        self.assertEqual(self.expected_pod['metadata']['labels'], actual_pod['metadata']['labels'])

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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        self.assertEqual(self.expected_pod['spec'], actual_pod['spec'])
        self.assertEqual(self.expected_pod['metadata']['labels'], actual_pod['metadata']['labels'])

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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        self.expected_pod['spec']['dnsPolicy'] = dns_policy
        self.assertEqual(self.expected_pod['spec'], actual_pod['spec'])
        self.assertEqual(self.expected_pod['metadata']['labels'], actual_pod['metadata']['labels'])

    def test_pod_schedulername(self):
        scheduler_name = "default-scheduler"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            schedulername=scheduler_name
        )
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['schedulerName'] = scheduler_name
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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['nodeSelector'] = node_selectors
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_resources(self):
        resources = {
            'limit_cpu': 0.25,
            'limit_memory': '64Mi',
            'limit_ephemeral_storage': '2Gi',
            'request_cpu': '250m',
            'request_memory': '64Mi',
            'request_ephemeral_storage': '1Gi',
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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['resources'] = {
            'requests': {
                'memory': '64Mi',
                'cpu': '250m',
                'ephemeral-storage': '1Gi'
            },
            'limits': {
                'memory': '64Mi',
                'cpu': 0.25,
                'nvidia.com/gpu': None,
                'ephemeral-storage': '2Gi'
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
        context = self.create_context(k)
        k.execute(context=context)
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
        context = self.create_context(k)
        k.execute(context=context)
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
            context = self.create_context(k)
            k.execute(context=context)
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
        context = self.create_context(k)
        k.execute(context)
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
        context = self.create_context(k)
        k.execute(context)
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
        context = self.create_context(k)
        k.execute(context)
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
            context = self.create_context(k)
            k.execute(context)
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
            context = self.create_context(k)
            k.execute(context)
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
            context = self.create_context(k)
            k.execute(context)
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
        context = self.create_context(k)
        self.assertEqual(k.execute(context), json.loads(return_value))
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(PodDefaults.SIDECAR_CONTAINER)
        self.expected_pod['spec']['containers'][0]['args'] = args
        self.expected_pod['spec']['containers'][0]['volumeMounts'].insert(0, volume_mount)
        self.expected_pod['spec']['volumes'].insert(0, volume)
        self.expected_pod['spec']['containers'].append(container)
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_monitor, mock_start):
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
        mock_monitor.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context)
        self.assertEqual(
            mock_start.call_args[0][0].spec.containers[0].env_from,
            [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(
                name=configmap
            ))]
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_secrets(self, mock_client, monitor_mock, start_mock):
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
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context)
        self.assertEqual(
            start_mock.call_args[0][0].spec.containers[0].env_from,
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
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['initContainers'] = [expected_init_container]
        self.expected_pod['spec']['volumes'] = [{
            'name': 'test-volume',
            'persistentVolumeClaim': {
                'claimName': 'test-volume'
            }
        }]
        self.assertEqual(self.expected_pod, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_template_file(
            self,
            mock_client,
            monitor_mock,
            start_mock):  # pylint: disable=unused-argument
        from airflow.utils.state import State
        k = KubernetesPodOperator(
            task_id='task',
            pod_template_file='tests/kubernetes/pod.yaml',
            do_xcom_push=True
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': ANY, 'namespace': 'mem-example'},
            'spec': {
                'volumes': [{'name': 'xcom', 'emptyDir': {}}],
                'containers': [{
                    'args': ['--vm', '1', '--vm-bytes', '150M', '--vm-hang', '1'],
                    'command': ['stress'],
                    'image': 'polinux/stress',
                    'name': 'memory-demo-ctr',
                    'resources': {
                        'limits': {'memory': '200Mi'},
                        'requests': {'memory': '100Mi'}
                    },
                    'volumeMounts': [{
                        'name': 'xcom',
                        'mountPath': '/airflow/xcom'
                    }]
                }, {
                    'name': 'airflow-xcom-sidecar',
                    'image': "alpine",
                    'command': ['sh', '-c', PodDefaults.XCOM_CMD],
                    'volumeMounts': [
                        {
                            'name': 'xcom',
                            'mountPath': '/airflow/xcom'
                        }
                    ],
                    'resources': {'requests': {'cpu': '1m'}},
                }],
            }
        }, actual_pod)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_priority_class_name(
            self,
            mock_client,
            monitor_mock,
            start_mock):  # pylint: disable=unused-argument
        """Test ability to assign priorityClassName to pod

        """
        from airflow.utils.state import State

        priority_class_name = "medium-test"
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
            priority_class_name=priority_class_name,
        )

        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['priorityClassName'] = priority_class_name
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_name(self):
        pod_name_too_long = "a" * 221
        with self.assertRaises(AirflowException):
            KubernetesPodOperator(
                namespace='default',
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels={"foo": "bar"},
                name=pod_name_too_long,
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
            )


# pylint: enable=unused-argument
