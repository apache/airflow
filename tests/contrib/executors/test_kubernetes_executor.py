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
#

import unittest
import uuid

import mock
import re
import string
import random
from urllib3 import HTTPResponse
from datetime import datetime

try:
    from kubernetes.client.rest import ApiException
    from airflow.contrib.executors.kubernetes_executor import AirflowKubernetesScheduler
    from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor
    from airflow.contrib.executors.kubernetes_executor import KubernetesExecutorConfig
    from airflow.contrib.kubernetes.worker_configuration import WorkerConfiguration
except ImportError:
    AirflowKubernetesScheduler = None


class TestAirflowKubernetesScheduler(unittest.TestCase):
    @staticmethod
    def _gen_random_string(str_len):
        return ''.join([random.choice(string.printable) for _ in range(str_len)])

    def _cases(self):
        cases = [
            ("my_dag_id", "my-task-id"),
            ("my.dag.id", "my.task.id"),
            ("MYDAGID", "MYTASKID"),
            ("my_dag_id", "my_task_id"),
            ("mydagid" * 200, "my_task_id" * 200)
        ]

        cases.extend([
            (self._gen_random_string(200), self._gen_random_string(200))
            for _ in range(100)
        ])

        return cases

    @staticmethod
    def _is_valid_name(name):
        regex = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        return (
            len(name) <= 253 and
            all(ch.lower() == ch for ch in name) and
            re.match(regex, name))

    @unittest.skipIf(AirflowKubernetesScheduler is None,
                     'kubernetes python package is not installed')
    def test_create_pod_id(self):
        for dag_id, task_id in self._cases():
            pod_name = AirflowKubernetesScheduler._create_pod_id(dag_id, task_id)
            self.assertTrue(self._is_valid_name(pod_name))

    @unittest.skipIf(AirflowKubernetesScheduler is None,
                     "kubernetes python package is not installed")
    def test_execution_date_serialize_deserialize(self):
        datetime_obj = datetime.now()
        serialized_datetime = \
            AirflowKubernetesScheduler._datetime_to_label_safe_datestring(
                datetime_obj)
        new_datetime_obj = AirflowKubernetesScheduler._label_safe_datestring_to_datetime(
            serialized_datetime)

        self.assertEquals(datetime_obj, new_datetime_obj)


class TestKubernetesWorkerConfiguration(unittest.TestCase):
    """
    Tests that if dags_volume_subpath/logs_volume_subpath configuration
    options are passed to worker pod config
    """

    affinity_config = {
        'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [
                {
                    'topologyKey': 'kubernetes.io/hostname',
                    'labelSelector': {
                        'matchExpressions': [
                            {
                                'key': 'app',
                                'operator': 'In',
                                'values': ['airflow']
                            }
                        ]
                    }
                }
            ]
        }
    }

    tolerations_config = [
        {
            'key': 'dedicated',
            'operator': 'Equal',
            'value': 'airflow'
        },
        {
            'key': 'prod',
            'operator': 'Exists'
        }
    ]

    def setUp(self):
        if AirflowKubernetesScheduler is None:
            self.skipTest("kubernetes python package is not installed")

        self.resources = mock.patch(
            'airflow.contrib.kubernetes.worker_configuration.Resources'
        )
        self.secret = mock.patch(
            'airflow.contrib.kubernetes.worker_configuration.Secret'
        )

        for patcher in [self.resources, self.secret]:
            self.mock_foo = patcher.start()
            self.addCleanup(patcher.stop)

        self.kube_config = mock.MagicMock()
        self.kube_config.airflow_home = '/'
        self.kube_config.airflow_dags = 'dags'
        self.kube_config.airflow_dags = 'logs'
        self.kube_config.dags_volume_subpath = None
        self.kube_config.logs_volume_subpath = None
        self.kube_config.dags_in_image = False
        self.kube_config.dags_folder = None
        self.kube_config.git_dags_folder_mount_point = None

    def test_worker_configuration_no_subpaths(self):
        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()
        volumes_list = [value for value in volumes.values()]
        volume_mounts_list = [value for value in volume_mounts.values()]
        for volume_or_mount in volumes_list + volume_mounts_list:
            if volume_or_mount['name'] != 'airflow-config':
                self.assertNotIn(
                    'subPath', volume_or_mount,
                    "subPath shouldn't be defined"
                )

    def test_worker_with_subpaths(self):
        self.kube_config.dags_volume_subpath = 'dags'
        self.kube_config.logs_volume_subpath = 'logs'
        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        for volume in [value for value in volumes.values()]:
            self.assertNotIn(
                'subPath', volume,
                "subPath isn't valid configuration for a volume"
            )

        for volume_mount in [value for value in volume_mounts.values()]:
            if volume_mount['name'] != 'airflow-config':
                self.assertIn(
                    'subPath', volume_mount,
                    "subPath should've been passed to volumeMount configuration"
                )

    def test_worker_generate_dag_volume_mount_path(self):
        self.kube_config.git_dags_folder_mount_point = '/root/airflow/git/dags'
        self.kube_config.dags_folder = '/root/airflow/dags'
        worker_config = WorkerConfiguration(self.kube_config)

        self.kube_config.dags_volume_claim = 'airflow-dags'
        self.kube_config.dags_volume_host = ''
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path, self.kube_config.dags_folder)

        self.kube_config.dags_volume_claim = ''
        self.kube_config.dags_volume_host = '/host/airflow/dags'
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path, self.kube_config.dags_folder)

        self.kube_config.dags_volume_claim = ''
        self.kube_config.dags_volume_host = ''
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path,
                         self.kube_config.git_dags_folder_mount_point)

    def test_worker_environment_no_dags_folder(self):
        self.kube_config.airflow_configmap = ''
        self.kube_config.git_dags_folder_mount_point = ''
        self.kube_config.dags_folder = ''
        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertNotIn('AIRFLOW__CORE__DAGS_FOLDER', env)

    def test_worker_environment_when_dags_folder_specified(self):
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_dags_folder_mount_point = ''
        dags_folder = '/workers/path/to/dags'
        self.kube_config.dags_folder = dags_folder

        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertEqual(dags_folder, env['AIRFLOW__CORE__DAGS_FOLDER'])

    def test_worker_environment_dags_folder_using_git_sync(self):
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'dags'
        self.kube_config.git_dags_folder_mount_point = '/workers/path/to/dags'

        dags_folder = '{}/{}/{}'.format(self.kube_config.git_dags_folder_mount_point,
                                        self.kube_config.git_sync_dest,
                                        self.kube_config.git_subpath)

        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertEqual(dags_folder, env['AIRFLOW__CORE__DAGS_FOLDER'])

    def test_make_pod_with_empty_executor_config(self):
        self.kube_config.kube_affinity = self.affinity_config
        self.kube_config.kube_tolerations = self.tolerations_config

        worker_config = WorkerConfiguration(self.kube_config)
        kube_executor_config = KubernetesExecutorConfig(annotations=[],
                                                        volumes=[],
                                                        volume_mounts=[]
                                                        )

        pod = worker_config.make_pod("default", str(uuid.uuid4()), "test_pod_id", "test_dag_id",
                                     "test_task_id", str(datetime.utcnow()), "bash -c 'ls /'",
                                     kube_executor_config)

        self.assertTrue(pod.affinity['podAntiAffinity'] is not None)
        self.assertEqual('app',
                         pod.affinity['podAntiAffinity']
                         ['requiredDuringSchedulingIgnoredDuringExecution'][0]
                         ['labelSelector']
                         ['matchExpressions'][0]
                         ['key'])

        self.assertEqual(2, len(pod.tolerations))
        self.assertEqual('prod', pod.tolerations[1]['key'])

    def test_make_pod_with_executor_config(self):
        worker_config = WorkerConfiguration(self.kube_config)
        kube_executor_config = KubernetesExecutorConfig(affinity=self.affinity_config,
                                                        tolerations=self.tolerations_config,
                                                        annotations=[],
                                                        volumes=[],
                                                        volume_mounts=[]
                                                        )

        pod = worker_config.make_pod("default", str(uuid.uuid4()), "test_pod_id", "test_dag_id",
                                     "test_task_id", str(datetime.utcnow()), "bash -c 'ls /'",
                                     kube_executor_config)

        self.assertTrue(pod.affinity['podAntiAffinity'] is not None)
        self.assertEqual('app',
                         pod.affinity['podAntiAffinity']
                         ['requiredDuringSchedulingIgnoredDuringExecution'][0]
                         ['labelSelector']
                         ['matchExpressions'][0]
                         ['key'])

        self.assertEqual(2, len(pod.tolerations))
        self.assertEqual('prod', pod.tolerations[1]['key'])

    def test_worker_pvc_dags(self):
        # Tests persistence volume config created when `dags_volume_claim` is set
        self.kube_config.dags_volume_claim = 'airflow-dags'

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        init_containers = worker_config._get_init_containers(volume_mounts)

        dag_volume = [volume for volume in volumes.values() if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts.values() if mount['name'] == 'airflow-dags']

        self.assertEqual('airflow-dags', dag_volume[0]['persistentVolumeClaim']['claimName'])
        self.assertEqual(1, len(dag_volume_mount))
        self.assertTrue(dag_volume_mount[0]['readOnly'])
        self.assertEqual(0, len(init_containers))

    def test_worker_git_dags(self):
        # Tests persistence volume config created when `git_repo` is set
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_folder = '/usr/local/airflow/dags'
        self.kube_config.worker_dags_folder = '/usr/local/airflow/dags'

        self.kube_config.git_sync_container_repository = 'gcr.io/google-containers/git-sync-amd64'
        self.kube_config.git_sync_container_tag = 'v2.0.5'
        self.kube_config.git_sync_container = 'gcr.io/google-containers/git-sync-amd64:v2.0.5'
        self.kube_config.git_sync_init_container_name = 'git-sync-clone'
        self.kube_config.git_subpath = 'dags_folder'
        self.kube_config.git_sync_root = '/git'
        self.kube_config.git_dags_folder_mount_point = '/usr/local/airflow/dags/repo/dags_folder'

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        dag_volume = [volume for volume in volumes.values() if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts.values() if mount['name'] == 'airflow-dags']

        self.assertTrue('emptyDir' in dag_volume[0])
        self.assertEqual(self.kube_config.git_dags_folder_mount_point, dag_volume_mount[0]['mountPath'])
        self.assertTrue(dag_volume_mount[0]['readOnly'])

        init_container = worker_config._get_init_containers(volume_mounts)[0]
        init_container_volume_mount = [mount for mount in init_container['volumeMounts']
                                       if mount['name'] == 'airflow-dags']

        self.assertEqual('git-sync-clone', init_container['name'])
        self.assertEqual('gcr.io/google-containers/git-sync-amd64:v2.0.5', init_container['image'])
        self.assertEqual(1, len(init_container_volume_mount))
        self.assertFalse(init_container_volume_mount[0]['readOnly'])

    def test_worker_container_dags(self):
        # Tests that the 'airflow-dags' persistence volume is NOT created when `dags_in_image` is set
        self.kube_config.dags_in_image = True

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        dag_volume = [volume for volume in volumes.values() if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts.values() if mount['name'] == 'airflow-dags']

        init_containers = worker_config._get_init_containers(volume_mounts)

        self.assertEqual(0, len(dag_volume))
        self.assertEqual(0, len(dag_volume_mount))
        self.assertEqual(0, len(init_containers))


class TestKubernetesExecutor(unittest.TestCase):
    """
    Tests if an ApiException from the Kube Client will cause the task to
    be rescheduled.
    """
    @unittest.skipIf(AirflowKubernetesScheduler is None,
                     'kubernetes python package is not installed')
    @mock.patch('airflow.contrib.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.contrib.executors.kubernetes_executor.get_kube_client')
    def test_run_next_exception(self, mock_get_kube_client, mock_kubernetes_job_watcher):

        # When a quota is exceeded this is the ApiException we get
        r = HTTPResponse()
        r.body = {
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "pods \"podname\" is forbidden: " +
            "exceeded quota: compute-resources, " +
            "requested: limits.memory=4Gi, " +
            "used: limits.memory=6508Mi, " +
            "limited: limits.memory=10Gi",
            "reason": "Forbidden",
            "details": {"name": "podname", "kind": "pods"},
            "code": 403},
        r.status = 403
        r.reason = "Forbidden"

        # A mock kube_client that throws errors when making a pod
        mock_kube_client = mock.patch('kubernetes.client.CoreV1Api', autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock(
            side_effect=ApiException(http_resp=r))
        mock_get_kube_client.return_value = mock_kube_client

        kubernetesExecutor = KubernetesExecutor()
        kubernetesExecutor.start()

        # Execute a task while the Api Throws errors
        try_number = 1
        kubernetesExecutor.execute_async(key=('dag', 'task', datetime.utcnow(), try_number),
                                         command='command', executor_config={})
        kubernetesExecutor.sync()
        kubernetesExecutor.sync()

        mock_kube_client.create_namespaced_pod.assert_called()
        self.assertFalse(kubernetesExecutor.task_queue.empty())

        # Disable the ApiException
        mock_kube_client.create_namespaced_pod.side_effect = None

        # Execute the task without errors should empty the queue
        kubernetesExecutor.sync()
        mock_kube_client.create_namespaced_pod.assert_called()
        self.assertTrue(kubernetesExecutor.task_queue.empty())


if __name__ == '__main__':
    unittest.main()
