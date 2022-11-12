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

from unittest import mock
from unittest.mock import MagicMock

from airflow.providers.google.cloud.operators.cloud_run import CloudRunOperator

TASK_ID = "task-id"
HTTP_CONN_ID = "test-http-conn"
GCP_CONN_ID = "test-conn"
AUTHENTICATION_HEADER = {"Authorisation": "Bearer TOKEN"}


class TestCloudRunOperator:
    @mock.patch("airflow.providers.google.cloud.operators.cloud_run.CloudRunHook")
    @mock.patch("airflow.providers.google.cloud.operators.cloud_run.SimpleHttpOperator.execute")
    def test_execute(self, mock_http_execute, mock_cloudrun_hook):
        mock_cloudrun_hook.return_value.get_conn.return_value = AUTHENTICATION_HEADER
        op = CloudRunOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            http_conn_id=HTTP_CONN_ID,
        )
        op.execute(context=MagicMock())

        mock_cloudrun_hook.return_value.get_conn.assert_called_once()
        mock_http_execute.assert_called_once()
        assert op.headers == AUTHENTICATION_HEADER
