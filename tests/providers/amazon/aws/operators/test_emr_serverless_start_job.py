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
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

MOCK_DATA = {
    'application_id': 'test_emr_serverless_create_application_operator',
    'execution_role_arn': 'test_emr_serverless_role_arn',
    'task_id': 'test_emr_serverless_task_id',
    'job_driver': {'test_key': 'test_value'},
    'configuration_overrides': {'monitoringConfiguration': {'test_key': 'test_value'}},
    'client_request_token': 'eac427d0-1c6d-4dfb-96aa-32423412',
    'job_run_id': 'test_job_run_id',
}


class TestEmrServerlessStartJobOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_app_started_with_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': MOCK_DATA['job_run_id'],
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=MOCK_DATA['task_id'],
            client_request_token=MOCK_DATA['client_request_token'],
            application_id=MOCK_DATA['application_id'],
            execution_role_arn=MOCK_DATA['execution_role_arn'],
            job_driver=MOCK_DATA['job_driver'],
            configuration_overrides=MOCK_DATA['configuration_overrides'],
        )

        id = operator.execute(None)
        assert operator.wait_for_completion is True

        mock_conn.get_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            applicationId=MOCK_DATA['application_id'],
            executionRoleArn=MOCK_DATA['execution_role_arn'],
            jobDriver=MOCK_DATA['job_driver'],
            configurationOverrides=MOCK_DATA['configuration_overrides'],
        )

        mock_waiter.assert_called_once()

        assert id == MOCK_DATA['job_run_id']

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_app_not_started_with_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': MOCK_DATA['job_run_id'],
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=MOCK_DATA['task_id'],
            client_request_token=MOCK_DATA['client_request_token'],
            application_id=MOCK_DATA['application_id'],
            execution_role_arn=MOCK_DATA['execution_role_arn'],
            job_driver=MOCK_DATA['job_driver'],
            configuration_overrides=MOCK_DATA['configuration_overrides'],
        )

        id = operator.execute(None)
        assert operator.wait_for_completion is True

        mock_conn.get_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            applicationId=MOCK_DATA['application_id'],
            executionRoleArn=MOCK_DATA['execution_role_arn'],
            jobDriver=MOCK_DATA['job_driver'],
            configurationOverrides=MOCK_DATA['configuration_overrides'],
        )

        assert mock_waiter.call_count == 2

        assert id == MOCK_DATA['job_run_id']

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_app_not_started_without_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': MOCK_DATA['job_run_id'],
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=MOCK_DATA['task_id'],
            client_request_token=MOCK_DATA['client_request_token'],
            application_id=MOCK_DATA['application_id'],
            execution_role_arn=MOCK_DATA['execution_role_arn'],
            job_driver=MOCK_DATA['job_driver'],
            configuration_overrides=MOCK_DATA['configuration_overrides'],
            wait_for_completion=False,
        )

        id = operator.execute(None)

        mock_conn.get_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            applicationId=MOCK_DATA['application_id'],
            executionRoleArn=MOCK_DATA['execution_role_arn'],
            jobDriver=MOCK_DATA['job_driver'],
            configurationOverrides=MOCK_DATA['configuration_overrides'],
        )

        mock_waiter.assert_called_once()

        assert id == MOCK_DATA['job_run_id']

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_start_job_run(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': MOCK_DATA['job_run_id'],
            'ResponseMetadata': {'HTTPStatusCode': 404},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=MOCK_DATA['task_id'],
            client_request_token=MOCK_DATA['client_request_token'],
            application_id=MOCK_DATA['application_id'],
            execution_role_arn=MOCK_DATA['execution_role_arn'],
            job_driver=MOCK_DATA['job_driver'],
            configuration_overrides=MOCK_DATA['configuration_overrides'],
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "EMR serverless job failed to start:" in str(ex_message.value)

        mock_conn.get_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=MOCK_DATA['client_request_token'],
            applicationId=MOCK_DATA['application_id'],
            executionRoleArn=MOCK_DATA['execution_role_arn'],
            jobDriver=MOCK_DATA['job_driver'],
            configurationOverrides=MOCK_DATA['configuration_overrides'],
        )

        mock_waiter.assert_called_once()
