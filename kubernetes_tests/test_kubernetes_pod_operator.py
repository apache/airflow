# pylint: disable=unused-argument
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
import logging
import os
import shutil
import sys
import unittest
import textwrap

import kubernetes.client.models as k8s
import pendulum
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG, TaskInstance
from airflow.utils import timezone
from airflow.version import version as airflow_version
from tests.compat import mock, patch


# noinspection DuplicatedCode
def create_context(task):
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


def get_kubeconfig_path():
    kubeconfig_path = os.environ.get('KUBECONFIG')
    return kubeconfig_path if kubeconfig_path else os.path.expanduser('~/.kube/config')


# noinspection DuplicatedCode,PyUnusedLocal
class TestKubernetesPodOperatorSystem(unittest.TestCase):
    def get_current_task_name(self):
        # reverse test name to make pod name unique (it has limited length)
        return "_" + unittest.TestCase.id(self).replace(".", "_")[::-1]

    def setUp(self):
        self.maxDiff = None  # pylint: disable=invalid-name
        self.api_client = ApiClient()
        self.expected_pod = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'namespace': 'default',
                'name': mock.ANY,
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

    def tearDown(self):
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
        old_config_path = get_kubeconfig_path()
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
        old_config_path = get_kubeconfig_path()
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
        context = create_context(k)
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

    def test_pod_with_volume_secret(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            in_cluster=False,
            labels={"foo": "bar"},
            arguments=["echo 10"],
            secrets=[Secret(
                deploy_type="volume",
                deploy_target="/var/location",
                secret="my-secret",
                key="content.json",
            )],
            name="airflow-test-pod",
            task_id="task",
            get_logs=True,
            is_delete_operator_pod=True,
        )

        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['volumeMounts'] = [
            {'mountPath': '/var/location',
             'name': mock.ANY,
             'readOnly': True}]
        self.expected_pod['spec']['volumes'] = [
            {'name': mock.ANY,
             'secret': {'secretName': 'my-secret'}}
        ]
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
        self.assertEqual(self.expected_pod['spec'], actual_pod['spec'])
        self.assertEqual(self.expected_pod['metadata']['labels'], actual_pod['metadata']['labels'])

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
        context = create_context(k)
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
        context = create_context(k)
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
        with patch.object(PodLauncher, 'log') as mock_logger:
            volume_mount = VolumeMount('test-volume',
                                       mount_path='/tmp/test_volume',
                                       sub_path=None,
                                       read_only=False)

            volume_config = {
                'persistentVolumeClaim':
                    {
                        'claimName': 'test-volume'
                    }
            }
            volume = Volume(name='test-volume', configs=volume_config)
            args = ["echo \"retrieved from mount\" > /tmp/test_volume/test.txt "
                    "&& cat /tmp/test_volume/test.txt"]
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
            context = create_context(k)
            k.execute(context=context)
            mock_logger.info.assert_any_call(b"retrieved from mount\n")
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['args'] = args
            self.expected_pod['spec']['containers'][0]['volumeMounts'] = [{
                'name': 'test-volume',
                'mountPath': '/tmp/test_volume',
                'readOnly': False
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
        context = create_context(k)
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
        context = create_context(k)
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
        context = create_context(k)
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
            context = create_context(k)
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
            context = create_context(k)
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
            context = create_context(k)
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
        context = create_context(k)
        self.assertEqual(k.execute(context), json.loads(return_value))
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(PodDefaults.SIDECAR_CONTAINER)
        self.expected_pod['spec']['containers'][0]['args'] = args
        self.expected_pod['spec']['containers'][0]['volumeMounts'].insert(0, volume_mount)  # noqa
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

    def test_env_vars(self):
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars={"ENV1": "val1", "ENV2": "val2", },
            pod_runtime_info_envs=[PodRuntimeInfoEnv("ENV3", "status.podIP")],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )

        context = create_context(k)
        k.execute(context)

        # THEN
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['env'] = [
            {'name': 'ENV1', 'value': 'val1'},
            {'name': 'ENV2', 'value': 'val2'},
            {
                'name': 'ENV3',
                'valueFrom': {
                    'fieldRef': {
                        'fieldPath': 'status.podIP'
                    }
                }
            }
        ]
        self.assertEqual(self.expected_pod, actual_pod)

    def test_pod_template_file_system(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True
        )

        context = create_context(k)
        result = k.execute(context)
        self.assertIsNotNone(result)
        self.assertDictEqual(result, {"hello": "world"})

    def test_pod_template_file_with_overrides_system(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            labels={"foo": "bar", "fizz": "buzz"},
            env_vars={"env_name": "value"},
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True
        )

        context = create_context(k)
        result = k.execute(context)
        self.assertIsNotNone(result)
        self.assertEqual(k.pod.metadata.labels, {'fizz': 'buzz', 'foo': 'bar'})
        self.assertEqual(k.pod.spec.containers[0].env, [k8s.V1EnvVar(name="env_name", value="value")])
        self.assertDictEqual(result, {"hello": "world"})

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
        context = create_context(k)
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
    def test_pod_template_file(self, mock_client, monitor_mock, start_mock):
        from airflow.utils.state import State
        fixture = sys.path[0] + '/tests/kubernetes/pod.yaml'
        k = KubernetesPodOperator(
            task_id='task',
            pod_template_file=fixture,
            do_xcom_push=True
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        context = create_context(k)
        with self.assertLogs(k.log, level=logging.DEBUG) as cm:
            k.execute(context)
            expected_line = textwrap.dedent("""\
            DEBUG:airflow.task.operators:Starting pod:
            api_version: v1
            kind: Pod
            metadata:
              annotations: {}
              cluster_name: null
              creation_timestamp: null
              deletion_grace_period_seconds: null\
            """).strip()
            self.assertTrue(any(line.startswith(expected_line) for line in cm.output))

        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        expected_dict = {'apiVersion': 'v1',
                         'kind': 'Pod',
                         'metadata': {'annotations': {},
                                      'labels': {},
                                      'name': 'memory-demo',
                                      'namespace': 'mem-example'},
                         'spec': {'affinity': {},
                                  'containers': [{'args': ['--vm',
                                                           '1',
                                                           '--vm-bytes',
                                                           '150M',
                                                           '--vm-hang',
                                                           '1'],
                                                  'command': ['stress'],
                                                  'env': [],
                                                  'envFrom': [],
                                                  'image': 'apache/airflow:stress-2020.07.10-1.0.4',
                                                  'imagePullPolicy': 'IfNotPresent',
                                                  'name': 'base',
                                                  'ports': [],
                                                  'resources': {'limits': {'memory': '200Mi'},
                                                                'requests': {'memory': '100Mi'}},
                                                  'volumeMounts': [{'mountPath': '/airflow/xcom',
                                                                    'name': 'xcom'}]},
                                                 {'command': ['sh',
                                                              '-c',
                                                              'trap "exit 0" INT; while true; do sleep '
                                                              '30; done;'],
                                                  'image': 'alpine',
                                                  'name': 'airflow-xcom-sidecar',
                                                  'resources': {'requests': {'cpu': '1m'}},
                                                  'volumeMounts': [{'mountPath': '/airflow/xcom',
                                                                    'name': 'xcom'}]}],
                                  'hostNetwork': False,
                                  'imagePullSecrets': [],
                                  'initContainers': [],
                                  'nodeSelector': {},
                                  'restartPolicy': 'Never',
                                  'securityContext': {},
                                  'serviceAccountName': 'default',
                                  'tolerations': [],
                                  'volumes': [{'emptyDir': {}, 'name': 'xcom'}]}}
        self.assertEqual(expected_dict, actual_pod)

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

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    def test_on_kill(self,
                     monitor_mock):  # pylint: disable=unused-argument
        from airflow.utils.state import State
        client = kube_client.get_kube_client(in_cluster=False)
        name = "test"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["sleep 1000"],
            labels={"foo": "bar"},
            name="test",
            task_id=name,
            in_cluster=False,
            do_xcom_push=False,
            termination_grace_period=0,
        )
        context = create_context(k)
        monitor_mock.return_value = (State.SUCCESS, None)
        k.execute(context)
        name = k.pod.metadata.name
        pod = client.read_namespaced_pod(name=name, namespace=namespace)
        self.assertEqual(pod.status.phase, "Running")
        k.on_kill()
        with self.assertRaises(ApiException):
            pod = client.read_namespaced_pod(name=name, namespace=namespace)

    def test_reattach_failing_pod_once(self):
        from airflow.utils.state import State
        client = kube_client.get_kube_client(in_cluster=False)
        name = "test"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["exit 1"],
            labels={"foo": "bar"},
            name="test",
            task_id=name,
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=False,
            termination_grace_period=0,
        )

        context = create_context(k)

        with mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod") as monitor_mock:
            monitor_mock.return_value = (State.SUCCESS, None)
            k.execute(context)
            name = k.pod.metadata.name
            pod = client.read_namespaced_pod(name=name, namespace=namespace)
            while pod.status.phase != "Failed":
                pod = client.read_namespaced_pod(name=name, namespace=namespace)
        with self.assertRaises(AirflowException):
            k.execute(context)
        pod = client.read_namespaced_pod(name=name, namespace=namespace)
        self.assertEqual(pod.metadata.labels["already_checked"], "True")
        with mock.patch("airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator"
                        ".create_new_pod_for_operator") as create_mock:
            create_mock.return_value = ("success", {}, {})
            k.execute(context)
            create_mock.assert_called_once()

# pylint: enable=unused-argument
