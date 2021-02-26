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
#
import unittest
import pytest
import requests
import requests_mock
from unittest import mock


from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.version import version

from airflow.providers.airbyte.hooks.airbyte import AirbyteHook, AirbyteJobController

AIRFLOW_VERSION = "v" + version.replace(".", "-").replace("+", "-")
AIRBYTE_STRING = "airflow.providers.airbyte.hooks.{}"

AIRBYTE_CONN_ID = 'test'
CONNECTION_ID = {"connectionId": "test"}
JOB_ID = 1


def get_airbyte_connection(unused_conn_id=None):
    return Connection(conn_id='test', conn_type='http', host='test:8001/')


def mock_init(*args, **kwargs):
    pass


class TestAirbyteHook(unittest.TestCase):
    """Test get, post and raise_for_status"""

    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        get_airbyte_connection()
        self.hook = AirbyteHook(
            airbyte_conn_id=AIRBYTE_CONN_ID
        )

    def return_value_get_job(self, status):
        response = mock.Mock()
        response.json.return_value = {'job': {'status': status}}
        return response

    @requests_mock.mock()
    def test_submit_job(self, m):
        m.post('http://test:8001/api/v1/connections/sync', status_code=200, text='{"job":{"id": 1}}', reason='OK')
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airbyte_connection):
            resp = self.hook.submit_job(connection_id=CONNECTION_ID)
            assert resp.text == '{"job":{"id": 1}}'

    @requests_mock.mock()
    def test_submit_job(self, m):
        m.post('http://test:8001/api/v1/jobs/get', status_code=200, text='{"job":{"status": "succeeded"}}', reason='OK')
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airbyte_connection):
            resp = self.hook.get_job(job_id=JOB_ID)
            assert resp.text == '{"job":{"status": "succeeded"}}'

    @mock.patch('plugin.hook.AirbyteHook.get_job')
    def test_wait_for_job(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(AirbyteJobController.SUCCEEDED)
        ]
        self.hook.wait_for_job(job_id=JOB_ID, wait_time=0)
        mock_get_job.assert_called_once_with(job_id=JOB_ID)

    @mock.patch('plugin.hook.AirbyteHook.get_job')
    def test_wait_for_job_error(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(AirbyteJobController.RUNNING),
            self.return_value_get_job(AirbyteJobController.ERROR)
        ]
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(job_id=JOB_ID, wait_time=0)

        calls = [mock.call(job_id=JOB_ID), mock.call(job_id=JOB_ID)]
        assert mock_get_job.has_calls(calls)

    @mock.patch('plugin.hook.AirbyteHook.get_job')
    def test_wait_for_job_timeout(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(AirbyteJobController.RUNNING),
            self.return_value_get_job(AirbyteJobController.RUNNING)
        ]
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(job_id=JOB_ID, wait_time=2, timeout=1)

        calls = [mock.call(job_id=JOB_ID), mock.call(job_id=JOB_ID)]
        assert mock_get_job.has_calls(calls)
