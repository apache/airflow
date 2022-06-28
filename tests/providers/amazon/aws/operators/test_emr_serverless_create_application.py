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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator

MOCK_DATA = {
    'task_id': 'test_emr_serverless_create_application_operator',
    'application_id': 'test_application_id',
    'release_label': 'test',
    'job_type': 'test',
    'client_request_token': 'eac427d0-1c6d-4dfb-96aa-32423412',
    'config': {'name': 'test_application_emr_serverless'},
}


class TestEmrServerlessCreateApplicationOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_execute_successfully_with_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": MOCK_DATA['application_id'],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=MOCK_DATA['task_id'],
            release_label=MOCK_DATA['release_label'],
            job_type=MOCK_DATA['job_type'],
            client_request_token=MOCK_DATA['client_request_token'],
            config=MOCK_DATA['config'],
        )

        id = operator.execute(None)

        mock_conn.create_application.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            releaseLabel=MOCK_DATA['release_label'],
            type=MOCK_DATA['job_type'],
            **MOCK_DATA['config'],
        )
        mock_conn.start_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        assert mock_waiter.call_count == 2
        assert id == MOCK_DATA['application_id']

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_execute_successfully_without_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": MOCK_DATA['application_id'],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=MOCK_DATA['task_id'],
            release_label=MOCK_DATA['release_label'],
            job_type=MOCK_DATA['job_type'],
            client_request_token=MOCK_DATA['client_request_token'],
            wait_for_completion=False,
            config=MOCK_DATA['config'],
        )

        id = operator.execute(None)

        mock_conn.create_application.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            releaseLabel=MOCK_DATA['release_label'],
            type=MOCK_DATA['job_type'],
            **MOCK_DATA['config'],
        )
        mock_conn.start_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        mock_waiter.assert_called_once()
        assert id == MOCK_DATA['application_id']

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_create_application(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": MOCK_DATA['application_id'],
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=MOCK_DATA['task_id'],
            release_label=MOCK_DATA['release_label'],
            job_type=MOCK_DATA['job_type'],
            client_request_token=MOCK_DATA['client_request_token'],
            config=MOCK_DATA['config'],
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application Creation failed:" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            releaseLabel=MOCK_DATA['release_label'],
            type=MOCK_DATA['job_type'],
            **MOCK_DATA['config'],
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_application_in_failure_state(self, mock_conn):
        '''MOVE TO HOOK TEST'''
        fail_state = "STOPPED"
        mock_conn.get_application.return_value = {"application": {"state": fail_state}}
        mock_conn.create_application.return_value = {
            "applicationId": MOCK_DATA['application_id'],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=MOCK_DATA['task_id'],
            release_label=MOCK_DATA['release_label'],
            job_type=MOCK_DATA['job_type'],
            client_request_token=MOCK_DATA['client_request_token'],
            config=MOCK_DATA['config'],
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert str(ex_message.value) == f"Application reached failure state {fail_state}."

        mock_conn.create_application.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            releaseLabel=MOCK_DATA['release_label'],
            type=MOCK_DATA['job_type'],
            **MOCK_DATA['config'],
        )
