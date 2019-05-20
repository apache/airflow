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

from requests.exceptions import BaseHTTPError

from airflow import AirflowException
from airflow.kubernetes.pod_launcher import PodLauncher

import unittest
import mock


class TestPodLauncher(unittest.TestCase):

    def setUp(self):
        self.mock_kube_client = mock.Mock()
        self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)

    def test_read_pod_logs_successfully_returns_logs(self):
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(mock.sentinel.logs, logs)

    def test_read_pod_logs_retries_successfully(self):
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.logs
        ]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(mock.sentinel.logs, logs)
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls([
            mock.call(
                _preload_content=False,
                container='base',
                follow=True,
                name=mock.sentinel.name,
                namespace=mock.sentinel.namespace,
                tail_lines=10
            ),
            mock.call(
                _preload_content=False,
                container='base',
                follow=True,
                name=mock.sentinel.name,
                namespace=mock.sentinel.namespace,
                tail_lines=10
            )
        ])

    def test_read_pod_logs_retries_fails(self):
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom')
        ]
        self.assertRaises(
            AirflowException,
            self.pod_launcher.read_pod_logs,
            mock.sentinel
        )

    def test_read_pod_returns_logs(self):
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)

    def test_read_pod_retries_successfully(self):
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.pod_info
        ]
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)
        self.mock_kube_client.read_namespaced_pod.assert_has_calls([
            mock.call(mock.sentinel.name, mock.sentinel.namespace),
            mock.call(mock.sentinel.name, mock.sentinel.namespace)
        ])

    def test_read_pod_retries_fails(self):
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom')
        ]
        self.assertRaises(
            AirflowException,
            self.pod_launcher.read_pod,
            mock.sentinel
        )
