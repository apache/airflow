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
from tests.compat import mock

from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow import AirflowException
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.pod import Port
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

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
                'name': mock.ANY,
                'annotations': {},
                'labels': {'foo': 'bar'}
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

    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.contrib.kubernetes.kube_client.get_kube_client")
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

    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.contrib.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, mock_launcher):
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
            image_pull_secrets=fake_pull_secrets,
            in_cluster=False,
            cluster_context='default'
        )
        mock_launcher.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(mock_launcher.call_args[0][0].image_pull_secrets,
                         fake_pull_secrets)

    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.delete_pod")
    @mock.patch("airflow.contrib.kubernetes.kube_client.get_kube_client")
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
            xcom_push=True
        )
        self.assertEqual(k.execute(None), json.loads(return_value))

    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.contrib.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_launcher):
        # GIVEN
        from airflow.utils.state import State
        configmaps = ['test-configmap']
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            configmaps=configmaps
        )
        # THEN
        mock_launcher.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(mock_launcher.call_args[0][0].configmaps, configmaps)

    @mock.patch("airflow.contrib.kubernetes.pod_launcher.PodLauncher.run_pod")
    @mock.patch("airflow.contrib.kubernetes.kube_client.get_kube_client")
    def test_envs_from_secrets(self, mock_client, mock_launcher):
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
        )
        # THEN
        mock_launcher.return_value = (State.SUCCESS, None)
        k.execute(None)
        self.assertEqual(mock_launcher.call_args[0][0].secrets, secrets)


# pylint: enable=unused-argument
if __name__ == '__main__':
    unittest.main()
