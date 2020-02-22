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
import datetime
import unittest

import mock
from kubernetes import client
from requests.exceptions import BaseHTTPError
from tenacity import wait_none

from airflow.exceptions import AirflowException
from airflow.kubernetes import pod_launcher


class TestPodLauncher(unittest.TestCase):

    def setUp(self):
        self.mock_kube_client = mock.create_autospec(client.CoreV1Api())
        self.pod_launcher = pod_launcher.PodLauncher(kube_client=self.mock_kube_client)
        self.pod_launcher._request_pod_log_chunk.retry.wait = wait_none()  # pylint: disable=no-member

        self.log_chunk_returns = (
            b"2020-01-01T00:00:00.123456 foo",
            b"2020-01-01T00:00:01.123456 bar",
            b"2020-01-01T00:00:02.123456 baz",
        )

    def test_read_pod_log_chunk_from_beginning(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = self.log_chunk_returns
        log_chunk = self.pod_launcher._read_pod_log_chunk(mock.sentinel, "")
        for log_line, expected in zip(log_chunk, self.log_chunk_returns):
            self.assertEqual(log_line, expected)

    def test_read_pod_log_chunk_with_last_line(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = self.log_chunk_returns
        log_chunk = self.pod_launcher._read_pod_log_chunk(mock.sentinel, self.log_chunk_returns[0])
        for log_line, expected in zip(log_chunk, self.log_chunk_returns[1:]):
            self.assertEqual(log_line, expected)

    def test_read_pod_log_chunk_with_last_line_not_in_returned_chunk(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = self.log_chunk_returns
        log_chunk = self.pod_launcher._read_pod_log_chunk(mock.sentinel, b"2020-01-02T00:00:02.123456 baz")
        for log_line, expected in zip(log_chunk, self.log_chunk_returns):
            self.assertEqual(log_line, expected)

    @mock.patch.object(pod_launcher, "dt", mock.Mock(wraps=datetime.datetime))
    def test_read_pod_log_chunk_retries_successfully(self):
        pod_launcher.dt.utcnow.return_value = datetime.datetime(2020, 1, 1)
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            self.log_chunk_returns,
        ]
        log_chunk = self.pod_launcher._read_pod_log_chunk(mock.sentinel, "")
        for log_line, expected in zip(log_chunk, self.log_chunk_returns):
            self.assertEqual(log_line, expected)

        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls([
            mock.call(
                _preload_content=False,
                timestamps=True,
                container='base',
                follow=False,
                since_seconds=1577836815,
                name=mock.sentinel.metadata.name,
                namespace=mock.sentinel.metadata.namespace,
            ),
            mock.call(
                _preload_content=False,
                timestamps=True,
                container='base',
                follow=False,
                since_seconds=1577836815,
                name=mock.sentinel.metadata.name,
                namespace=mock.sentinel.metadata.namespace,
            )
        ])

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = self.log_chunk_returns
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(list(logs), [log_line.split(b" ")[1] for log_line in self.log_chunk_returns])

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            self.log_chunk_returns,
        ]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        for log_line, expected_log_line in zip(logs, self.log_chunk_returns):
            self.assertEqual(log_line, expected_log_line.split(b" ")[1])

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom')
        ]
        with self.assertRaises(AirflowException):
            list(self.pod_launcher.read_pod_logs(mock.sentinel))

    def test_read_pod_events_successfully_returns_events(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.return_value = mock.sentinel.events
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        self.assertEqual(mock.sentinel.events, events)

    def test_read_pod_events_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.events
        ]
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        self.assertEqual(mock.sentinel.events, events)
        self.mock_kube_client.list_namespaced_event.assert_has_calls([
            mock.call(
                namespace=mock.sentinel.metadata.namespace,
                field_selector="involvedObject.name={}".format(mock.sentinel.metadata.name)
            ),
            mock.call(
                namespace=mock.sentinel.metadata.namespace,
                field_selector="involvedObject.name={}".format(mock.sentinel.metadata.name)
            )
        ])

    def test_read_pod_events_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom')
        ]
        self.assertRaises(
            AirflowException,
            self.pod_launcher.read_pod_events,
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
