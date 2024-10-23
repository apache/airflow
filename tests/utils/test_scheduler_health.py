#
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
from __future__ import annotations

from http.server import BaseHTTPRequestHandler
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.utils.scheduler_health import HealthServer

pytestmark = pytest.mark.db_test


class MockServer(HealthServer):
    def __init__(self):
        # Overriding so we don't need to initialize with BaseHTTPRequestHandler.__init__ params
        pass

    def do_GET(self, path):
        self.path = path
        super().do_GET()


class TestSchedulerHealthServer:
    def setup_method(self) -> None:
        self.mock_server = MockServer()

    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    def test_incorrect_endpoint(self, mock_send_error):
        self.mock_server.do_GET("/incorrect")
        mock_send_error.assert_called_with(404)

    @mock.patch.object(BaseHTTPRequestHandler, "end_headers")
    @mock.patch.object(BaseHTTPRequestHandler, "send_response")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_healthy_scheduler(self, mock_session, mock_send_response, mock_end_headers):
        mock_scheduler_job = MagicMock()
        mock_scheduler_job.is_alive.return_value = True
        mock_session.return_value.__enter__.return_value.query.return_value = mock_scheduler_job
        self.mock_server.do_GET("/health")
        mock_send_response.assert_called_once_with(200)
        mock_end_headers.assert_called_once()

    # This test is to ensure that if the scheduler is unhealthy, it returns a 503 error code.
    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_unhealthy_scheduler(self, mock_session, mock_send_error):
        mock_scheduler_job = MagicMock()
        mock_scheduler_job.is_alive.return_value = False
        mock_session.return_value.__enter__.return_value.query.return_value = mock_scheduler_job
        self.mock_server.do_GET("/health")
        mock_send_error.assert_called_with(503)

    # This test is to ensure that if there's no scheduler job running, it returns a 503 error code.
    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_missing_scheduler(self, mock_session, mock_send_error):
        mock_session.return_value.__enter__.return_value.query.return_value = None
        self.mock_server.do_GET("/health")
        mock_send_error.assert_called_with(503)
