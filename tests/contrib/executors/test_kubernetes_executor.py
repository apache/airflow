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
from datetime import datetime

try:
    from airflow.contrib.executors.kubernetes_executor import AirflowKubernetesScheduler
    from airflow.contrib.kubernetes.worker_configuration import WorkerConfiguration
except ImportError:
    AirflowKubernetesScheduler = None


class TestAirflowKubernetesScheduler(unittest.TestCase):
    def _gen_random_string(self, str_len):
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

    def _is_valid_name(self, name):
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


if __name__ == '__main__':
    unittest.main()
