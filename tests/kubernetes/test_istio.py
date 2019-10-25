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

from airflow import AirflowException
from airflow.contrib.kubernetes.istio import Istio

import unittest
from tests.compat import MagicMock, patch


def mock_stream(func, *args, **kwargs):
    print('calling func')
    return func(*args, **kwargs)


class TestIstio(unittest.TestCase):

    def setUp(self):
        mock_kube_client = MagicMock()
        self.istio = Istio(mock_kube_client)

    def _mock_pod(self, image="istio/proxyv2:1.3.0", args=[]):  # noqa
        sidecar = MagicMock()
        sidecar.name = "istio-proxy"
        sidecar.namespace = "fake-namespace"
        sidecar.image = image
        sidecar.args = args
        pod = MagicMock()
        pod.spec.containers = [sidecar]
        pod.status.phase = "Running"
        pod.metadata.name = "fake-pod-name"
        pod.metadata.namespace = "fake-namespace"
        container_status1 = MagicMock()
        container_status1.name = "istio-proxy"
        container_status1.state.running = True
        container_status1.state.terminated = False
        container_status2 = MagicMock()
        container_status2.name = "base"
        container_status2.state.running = False
        container_status2.state.terminated = True
        pod.status.container_statuses = [container_status1,
                                         container_status2]
        return pod

    def test_handle_istio_proxy_low_version(self):
        pod = self._mock_pod(image="istio/proxyv2:1.2.9")
        self.assertRaises(AirflowException,
                          self.istio.handle_istio_proxy,
                          pod)

    def _handle_istio_proxy_with_sidecar_args(self, args):
        pod = self._mock_pod(args=args)
        self.istio.handle_istio_proxy(pod)

    @patch("airflow.contrib.kubernetes.istio.stream", new=mock_stream)
    def test_handle_istio_proxy(self):
        args = ["proxy", "sidecar", "--statusPort", "12345"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.istio._client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                'fake-pod-name',
                'fake-namespace',
                tty=False,
                container='istio-proxy',
                stderr=True,
                stdin=False,
                stdout=True,
                command=['/bin/sh',
                         '-c',
                         'curl -XPOST http://127.0.0.1:12345/quitquitquit'])

    @patch("airflow.contrib.kubernetes.istio.stream", new=mock_stream)
    def test_handle_istio_proxy_other_cli_format(self):
        args = ["proxy", "sidecar", "--statusPort=12345"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.istio._client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                'fake-pod-name',
                'fake-namespace',
                tty=False,
                container='istio-proxy',
                stderr=True,
                stdin=False,
                stdout=True,
                command=['/bin/sh',
                         '-c',
                         'curl -XPOST http://127.0.0.1:12345/quitquitquit'])

    @patch("airflow.contrib.kubernetes.istio.stream", new=mock_stream)
    def test_handle_istio_proxy_no_cli_argument(self):
        args = ["proxy", "sidecar"]
        self._handle_istio_proxy_with_sidecar_args(args)
        self.istio._client.connect_get_namespaced_pod_exec.\
            assert_called_once_with(
                'fake-pod-name',
                'fake-namespace',
                tty=False,
                container='istio-proxy',
                stderr=True,
                stdin=False,
                stdout=True,
                command=['/bin/sh',
                         '-c',
                         'curl -XPOST http://127.0.0.1:15020/quitquitquit'])

    @patch("airflow.contrib.kubernetes.istio.stream", new=mock_stream)
    def test_handle_istio_with_no_sidecar(self):
        pod = MagicMock()
        pod.spec.containers = []
        self.istio.handle_istio_proxy(MagicMock())
        self.istio._client.connect_get_namespaced_pod_exec.\
            assert_not_called()


if __name__ == "__main__":
    unittest.main()
