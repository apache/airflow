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

import os
import tempfile
import unittest
from unittest import mock
from unittest.mock import MagicMock

import kubernetes

from airflow.cli import cli_parser
from airflow.cli.commands import kubernetes_command


class TestGenerateDagYamlCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def test_generate_dag_yaml(self):
        with tempfile.TemporaryDirectory("airflow_dry_run_test/") as directory:
            file_name = "example_bash_operator_run_after_loop_2020-11-03T00_00_00_plus_00_00.yml"
            kubernetes_command.generate_pod_yaml(
                self.parser.parse_args(
                    [
                        'kubernetes',
                        'generate-dag-yaml',
                        'example_bash_operator',
                        "2020-11-03",
                        "--output-path",
                        directory,
                    ]
                )
            )
            self.assertEqual(len(os.listdir(directory)), 1)
            out_dir = directory + "/airflow_yaml_output/"
            self.assertEqual(len(os.listdir(out_dir)), 6)
            self.assertTrue(os.path.isfile(out_dir + file_name))
            self.assertGreater(os.stat(out_dir + file_name).st_size, 0)


class TestCleanUpPodsCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('kubernetes.client.CoreV1Api.delete_namespaced_pod')
    def test_delete_pod(self, delete_namespaced_pod):
        kubernetes_command._delete_pod('dummy', 'awesome-namespace')
        delete_namespaced_pod.assert_called_with(body=mock.ANY, name='dummy', namespace='awesome-namespace')

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('airflow.kubernetes.kube_client.config.load_incluster_config')
    def test_running_pods_are_not_cleaned(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy'
        pod1.status.phase = 'Running'
        pod1.status.reason = None
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        delete_pod.assert_not_called()
        load_incluster_config.assert_called_once()

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('airflow.kubernetes.kube_client.config.load_incluster_config')
    def test_cleanup_succeeded_pods(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy'
        pod1.status.phase = 'Succeeded'
        pod1.status.reason = None
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        delete_pod.assert_called_with('dummy', 'awesome-namespace')
        load_incluster_config.assert_called_once()

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('kubernetes.config.load_incluster_config')
    def test_no_cleanup_failed_pods_wo_restart_policy_never(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy2'
        pod1.status.phase = 'Failed'
        pod1.status.reason = None
        pod1.spec.restart_policy = 'Always'
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        delete_pod.assert_not_called()
        load_incluster_config.assert_called_once()

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('kubernetes.config.load_incluster_config')
    def test_cleanup_failed_pods_w_restart_policy_never(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy3'
        pod1.status.phase = 'Failed'
        pod1.status.reason = None
        pod1.spec.restart_policy = 'Never'
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        delete_pod.assert_called_with('dummy3', 'awesome-namespace')
        load_incluster_config.assert_called_once()

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('kubernetes.config.load_incluster_config')
    def test_cleanup_evicted_pods(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy4'
        pod1.status.phase = 'Failed'
        pod1.status.reason = 'Evicted'
        pod1.spec.restart_policy = 'Never'
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        delete_pod.assert_called_with('dummy4', 'awesome-namespace')
        load_incluster_config.assert_called_once()

    @mock.patch('airflow.cli.commands.kubernetes_command._delete_pod')
    @mock.patch('kubernetes.client.CoreV1Api.list_namespaced_pod')
    @mock.patch('kubernetes.config.load_incluster_config')
    def test_cleanup_api_exception_continue(self, load_incluster_config, list_namespaced_pod, delete_pod):
        delete_pod.side_effect = kubernetes.client.rest.ApiException(status=0)
        pod1 = MagicMock()
        pod1.metadata.name = 'dummy'
        pod1.status.phase = 'Succeeded'
        pod1.status.reason = None
        pods = list_namespaced_pod()
        pods.metadata._continue = None
        pods.items = [pod1]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(['kubernetes', 'cleanup-pods', '--namespace', 'awesome-namespace'])
        )
        load_incluster_config.assert_called_once()
