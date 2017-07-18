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
import mock

from airflow import configuration, models
from airflow.contrib.hooks.kubernetes_hook import KubernetesHook
from airflow.exceptions import AirflowException
from airflow.utils import db
from mock import patch, call, Mock, MagicMock

from io import StringIO

class TestKubernetesHook(unittest.TestCase):
    def setUp(self):
        super(TestKubernetesHook, self).setUp()

        self.conn_mock = mock.MagicMock(name='coreV1Api')
        self.get_conn_orig = KubernetesHook.get_conn

        def _get_conn_mock(hook):
            hook.core_client = self.conn_mock
            return self.conn_mock

        KubernetesHook.get_conn = _get_conn_mock

    def tearDown(self):
        KubernetesHook.get_conn = self.get_conn_orig
        super(TestKubernetesHook, self).tearDown()

    def test_get_pod_definition(self):
        hook = KubernetesHook()

        pod = hook.get_pod_definition(name="test_pod", namespace="test_namespace", image="image:tag")
        self.assertTrue(pod.metadata.name == "test_pod")
        self.assertTrue(pod.metadata.namespace == "test_namespace")
        self.assertTrue(pod.metadata.labels is None)

        self.assertTrue(len(pod.spec.containers) == 1)
        self.assertTrue(pod.spec.containers[0].name == "test_pod")
        self.assertTrue(pod.spec.containers[0].image == "image:tag")
        self.assertTrue(pod.spec.containers[0].command is None)
        self.assertTrue(pod.spec.containers[0].args is None)

    def test_get_pod_definition_with_extras(self):
        hook = KubernetesHook()

        pod = hook.get_pod_definition(
                name="test_pod",
                namespace="test_namespace",
                image="image:tag",
                command=["python", "test.py"],
                args=["arg1", "arg2"],
                env={
                    'param1': 'string!',
                    'param2': {'source': 'configMap', 'name': 'config1', 'key': 'key1'},
                    'param3': {'source': 'secret', 'name': 'config1', 'key': 'key1'}},
                labels={"label1": "true", "label2": "true"})

        self.assertTrue(pod.metadata.name == "test_pod")
        self.assertTrue(pod.metadata.namespace == "test_namespace")
        self.assertTrue(pod.metadata.labels == {"label1": "true", "label2": "true"})

        self.assertTrue(len(pod.spec.containers) == 1)
        self.assertTrue(pod.spec.containers[0].name == "test_pod")
        self.assertTrue(pod.spec.containers[0].image == "image:tag")
        
        result = pod.spec.containers[0].env
        result.sort(key = lambda x: x.name)
        self.assertTrue(result[0].name == 'param1' and result[0].value == 'string!')
        self.assertTrue(result[1].name == 'param2' and
                result[1].value_from.config_map_key_ref.name == 'config1' and
                result[1].value_from.config_map_key_ref.key == 'key1')
        self.assertTrue(result[2].name == 'param3' and
                result[2].value_from.secret_key_ref.name == 'config1' and
                result[2].value_from.secret_key_ref.key == 'key1')

        self.assertTrue(pod.spec.containers[0].command == ["python", "test.py"])
        self.assertTrue(pod.spec.containers[0].args == ["arg1", "arg2"])

    def test_create_pod(self):
        hook = KubernetesHook()

        pod = Mock()

        hook.create_pod(pod)
        hook.get_conn().create_namespaced_pod.assert_called_once_with(pod.metadata.namespace, pod)

    def test_delete_pod(self):
        hook = KubernetesHook()

        pod = Mock()

        hook.delete_pod(pod)
        hook.get_conn().delete_namespaced_pod.assert_called_once_with(pod.metadata.name, pod.metadata.namespace, mock.ANY)

    def test_delete_pod_by_name(self):
        hook = KubernetesHook()

        hook.delete_pod(name="test_pod", namespace="test_namespace")
        hook.get_conn().delete_namespaced_pod.assert_called_once_with("test_pod", "test_namespace", mock.ANY)

    def test_get_pod_status_not_found(self):
        hook = KubernetesHook()

        hook.get_conn().read_namespaced_pod_status.return_value = None

        pod = Mock()
        with self.assertRaises(AirflowException) as context:
            hook.get_pod_state(pod)

        hook.get_conn().read_namespaced_pod_status.assert_called_once_with(pod.metadata.name, pod.metadata.namespace)

    def test_get_pod_status(self):
        hook = KubernetesHook()

        pod_mock = Mock()
        pod_mock.metadata.name = "test_pod"
        pod_mock.metadata.namespace = "test_namespace"
        pod_mock.status.phase = "Running"
        self.conn_mock.read_namespaced_pod_status.return_value = pod_mock

        state = hook.get_pod_state(pod_mock)

        self.assertTrue(state == "Running")
        hook.get_conn().read_namespaced_pod_status.assert_called_once_with(pod_mock.metadata.name, pod_mock.metadata.namespace)

    def test_get_pod_status_by_name(self):
        hook = KubernetesHook()

        pod_mock = Mock()
        pod_mock.metadata.name = "test_pod"
        pod_mock.metadata.namespace = "test_namespace"
        pod_mock.status.phase = "Running"
        self.conn_mock.read_namespaced_pod_status.return_value = pod_mock

        state = hook.get_pod_state(name=pod_mock.metadata.name, namespace=pod_mock.metadata.namespace)

        self.assertTrue(state == "Running")
        hook.get_conn().read_namespaced_pod_status.assert_called_once_with(pod_mock.metadata.name, pod_mock.metadata.namespace)

