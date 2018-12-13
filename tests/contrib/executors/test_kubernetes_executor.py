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

    def setUp(self):
        if AirflowKubernetesScheduler is None:
            self.skipTest("kubernetes python package is not installed")

        self.pod = mock.patch(
            'airflow.contrib.kubernetes.worker_configuration.Pod'
        )
        self.resources = mock.patch(
            'airflow.contrib.kubernetes.worker_configuration.Resources'
        )
        self.secret = mock.patch(
            'airflow.contrib.kubernetes.worker_configuration.Secret'
        )

        for patcher in [self.pod, self.resources, self.secret]:
            self.mock_foo = patcher.start()
            self.addCleanup(patcher.stop)

        self.kube_config = mock.MagicMock()
        self.kube_config.airflow_home = '/'
        self.kube_config.airflow_dags = 'dags'
        self.kube_config.airflow_dags = 'logs'
        self.kube_config.dags_volume_subpath = None
        self.kube_config.logs_volume_subpath = None
        self.kube_config.dags_in_image = False

    def test_worker_configuration_no_subpaths(self):
        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()
        for volume_or_mount in volumes + volume_mounts:
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

        for volume in volumes:
            self.assertNotIn(
                'subPath', volume,
                "subPath isn't valid configuration for a volume"
            )

        for volume_mount in volume_mounts:
            if volume_mount['name'] != 'airflow-config':
                self.assertIn(
                    'subPath', volume_mount,
                    "subPath should've been passed to volumeMount configuration"
                )

    def test_worker_environment_no_dags_folder(self):
        self.kube_config.worker_dags_folder = ''
        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertNotIn('AIRFLOW__CORE__DAGS_FOLDER', env)

    def test_worker_environment_when_dags_folder_specified(self):
        dags_folder = '/workers/path/to/dags'
        self.kube_config.worker_dags_folder = dags_folder

        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertEqual(dags_folder, env['AIRFLOW__CORE__DAGS_FOLDER'])

    def test_worker_pvc_dags(self):
        # Tests persistence volume config created when `dags_volume_claim` is set
        self.kube_config.dags_volume_claim = 'airflow-dags'

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        dag_volume = [volume for volume in volumes if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount['name'] == 'airflow-dags']

        self.assertEqual('airflow-dags', dag_volume[0]['persistentVolumeClaim']['claimName'])
        self.assertEqual(1, len(dag_volume_mount))

    def test_worker_git_dags(self):
        # Tests persistence volume config created when `git_repo` is set
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_folder = '/usr/local/airflow/dags'
        self.kube_config.worker_dags_folder = '/usr/local/airflow/dags'

        self.kube_config.git_sync_container_repository = 'gcr.io/google-containers/git-sync-amd64'
        self.kube_config.git_sync_container_tag = 'v2.0.5'
        self.kube_config.git_sync_container = 'gcr.io/google-containers/git-sync-amd64:v2.0.5'
        self.kube_config.git_sync_init_container_name = 'git-sync-clone'
        self.kube_config.git_subpath = ''

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        init_container = worker_config._get_init_containers(volume_mounts)[0]

        dag_volume = [volume for volume in volumes if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount['name'] == 'airflow-dags']

        self.assertTrue('emptyDir' in dag_volume[0])
        self.assertEqual('/usr/local/airflow/dags/', dag_volume_mount[0]['mountPath'])

        self.assertEqual('git-sync-clone', init_container['name'])
        self.assertEqual('gcr.io/google-containers/git-sync-amd64:v2.0.5', init_container['image'])

    def test_worker_container_dags(self):
        # Tests that the 'airflow-dags' persistence volume is NOT created when `dags_in_image` is set
        self.kube_config.dags_in_image = True

        worker_config = WorkerConfiguration(self.kube_config)
        volumes, volume_mounts = worker_config.init_volumes_and_mounts()

        dag_volume = [volume for volume in volumes if volume['name'] == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount['name'] == 'airflow-dags']

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
