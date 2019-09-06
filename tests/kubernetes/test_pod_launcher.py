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
from unittest.mock import MagicMock


class TestPodLauncher(unittest.TestCase):

    def setUp(self):
        self.mock_kube_client = mock.Mock()
        self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(mock.sentinel.logs, logs)

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
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
                name=mock.sentinel.metadata.name,
                namespace=mock.sentinel.metadata.namespace,
                tail_lines=10
            ),
            mock.call(
                _preload_content=False,
                container='base',
                follow=True,
                name=mock.sentinel.metadata.name,
                namespace=mock.sentinel.metadata.namespace,
                tail_lines=10
            )
        ])

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
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
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)

    def test_read_pod_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.pod_info
        ]
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)
        self.mock_kube_client.read_namespaced_pod.assert_has_calls([
            mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
            mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace)
        ])

    def test_read_pod_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
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

    def test_handle_istio_proxy_low_version(self):
        sidecar = MagicMock()
        sidecar.name = "istio-proxy"
        sidecar.image = "istio/proxyv2:1.2.9"
        pod = MagicMock()
        pod.spec.containers = [sidecar]
        self.mock_kube_client.read_namespaced_pod.return_value = pod
        self.assertRaises(
            AirflowException,
            self.pod_launcher._handle_istio_proxy,
            MagicMock()
        )

    def _handle_istio_proxy_with_sidecar_args(self, args):
        sidecar = MagicMock()
        sidecar.name = "istio-proxy"
        sidecar.image = "istio/proxyv2:1.3.0"
        sidecar.args = args
        pod = MagicMock()
        pod.spec.containers = [sidecar]
        pod.name = "fake-pod-name"
        pod.namespace = "fake-namespace"
        self.mock_kube_client.read_namespaced_pod.return_value = pod
        self.pod_launcher._handle_istio_proxy(MagicMock())

    def test_handle_istio_proxy(self):
        args = ["proxy",
                "sidecar",
                "--statusPort",
                "12345"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.mock_kube_client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                "fake-pod-name",
                "fake-namespace",
                container="istio-proxy",
                command=["curl",
                         "-XPOST",
                         "http://127.0.0.1:12345/quitquitquit"])

    def test_handle_istio_proxy_other_cli_format(self):
        args = ["proxy",
                "sidecar",
                "--statusPort=12345"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.mock_kube_client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                "fake-pod-name",
                "fake-namespace",
                container="istio-proxy",
                command=["curl",
                         "-XPOST",
                         "http://127.0.0.1:12345/quitquitquit"])

    def test_handle_istio_proxy_no_cli_argument(self):
        args = ["proxy",
                "sidecar"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.mock_kube_client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                "fake-pod-name",
                "fake-namespace",
                container="istio-proxy",
                command=["curl",
                         "-XPOST",
                         "http://127.0.0.1:15020/quitquitquit"])

    def test_handle_istio_with_no_sidecar(self):
        pod = MagicMock()
        pod.spec.containers = []
        self.mock_kube_client.read_namespaced_pod.return_value = pod
        self.pod_launcher._handle_istio_proxy(MagicMock())
        self.mock_kube_client.connect_get_namespaced_pod_exec.\
            assert_not_called()


if __name__ == "__main__":
    unittest.main()
