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

import unittest
import datetime
import sys

import mock
from mock import MagicMock, Mock

from airflow import DAG, configuration
from airflow.models import TaskInstance

from airflow.contrib.operators.kubernetes_operator import KubernetesPodOperator
from airflow.contrib.hooks.kubernetes_hook import KubernetesHook
from airflow.exceptions import AirflowException

DEFAULT_DATE = datetime.datetime(2017, 1, 1)
END_DATE = datetime.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)

class TestKubernetesOperator(unittest.TestCase):
    def setUp(self):
        super(TestKubernetesOperator, self).setUp()
        self._create_hook_orig = KubernetesPodOperator._create_hook
        
        self.context_mock = MagicMock()
        self.context_mock.__getitem__.side_effect = {'ti': type('', (object,), {'job_id': 1234})}.__getitem__

        self.pod_mock = MagicMock()
        self.pod_mock.metadata.name = "pod_name-1234"
        self.pod_mock.metadata.namespace = "pod_namespace"

        self.hook_mock = MagicMock(spec=KubernetesHook)

        def _create_hook_mock(sensor):
            return self.hook_mock

        KubernetesPodOperator._create_hook = _create_hook_mock

    def tearDown(self):
        TestKubernetesOperator._create_hook = self._create_hook_orig
        super(TestKubernetesOperator, self).tearDown()

    def test_should_do_cleanup(self):
        def get_operator(cleanup):
            return KubernetesPodOperator(
                    task_id="task",
                    name=None,
                    namespace=None,
                    image=None,
                    cleanup=cleanup)

        self.assertFalse(get_operator("Never").should_do_cleanup("Succeeded"))
        self.assertFalse(get_operator("Never").should_do_cleanup("Failed"))
        self.assertTrue(get_operator("Always").should_do_cleanup("Succeeded"))
        self.assertTrue(get_operator("Always").should_do_cleanup("Failed"))
        self.assertTrue(get_operator("OnSuccess").should_do_cleanup("Succeeded"))
        self.assertFalse(get_operator("OnSuccess").should_do_cleanup("Failed"))
        self.assertTrue(get_operator("OnFailure").should_do_cleanup("Failed"))
        self.assertFalse(get_operator("OnFailure").should_do_cleanup("Succeeded"))

    def test_execute_container_fails(self):
        self.hook_mock.reset_mock()
        self.context_mock.reset_mock()
        self.pod_mock.reset_mock()

        self.hook_mock.get_pod_definition.return_value = self.pod_mock
        self.hook_mock.get_pod_state.side_effect = ["Pending", "Failed", "Failed"]

        operator = KubernetesPodOperator(
                task_id='k8s_test',
                name="pod_name",
                namespace="pod_namespace",
                image="image:test",
                poke_interval=0,
                cleanup="Always")

        with self.assertRaises(AirflowException):
            operator.execute(self.context_mock)

        self.hook_mock.delete_pod.assert_called_once_with(self.pod_mock)

    def test_execute_container(self):
        self.hook_mock.reset_mock()
        self.context_mock.reset_mock()
        self.pod_mock.reset_mock()

        self.hook_mock.get_pod_definition.return_value = self.pod_mock
        self.hook_mock.get_pod_state.side_effect = ["Pending", "Running", "Succeeded"]

        operator = KubernetesPodOperator(
                task_id='k8s_test',
                name="pod_name",
                namespace="pod_namespace",
                image="image:test",
                poke_interval=0,
                cleanup="Always")

        operator.execute(self.context_mock)

        self.hook_mock.get_pod_definition.assert_called_once_with(
                args=None,
                command=None,
                image='image:test',
                labels=None,
                env=None,
                env_from=None,
                name="pod_name-1234",
                namespace='pod_namespace',
                restart_policy='Never',
                volumes=None)
        self.hook_mock.create_pod.assert_called_once_with(self.pod_mock)
        self.hook_mock.delete_pod.assert_called_once_with(self.pod_mock)
        self.assertEqual(self.hook_mock.get_pod_state.call_count, 3)

    def test_execute_container_no_wait(self):
        self.hook_mock.reset_mock()
        self.context_mock.reset_mock()
        self.pod_mock.reset_mock()

        self.hook_mock.get_pod_definition.return_value = self.pod_mock
        self.hook_mock.get_pod_state.side_effect = ["Pending", "Running", "Succeeded"]

        operator = KubernetesPodOperator(
                task_id='k8s_test',
                name="pod_name",
                namespace="pod_namespace",
                image="image:test",
                env={'param1': 'val'},
                poke_interval=0,
                cleanup="Never",
                wait=False)

        operator.execute(self.context_mock)

        self.hook_mock.get_pod_definition.assert_called_once_with(
                args=None,
                command=None,
                image='image:test',
                env={'param1': 'val'},
                env_from=None,
                labels=None,
                name='pod_name-1234',
                namespace='pod_namespace',
                restart_policy='Never',
                volumes=None)
        self.hook_mock.create_pod.assert_called_once_with(self.pod_mock)
        self.assertFalse(self.hook_mock.delete_pod.called)
        self.hook_mock.get_pod_state.called_twice_with(self.pod_mock)