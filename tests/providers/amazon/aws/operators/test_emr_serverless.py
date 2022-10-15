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
from uuid import UUID

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
)

task_id = 'test_emr_serverless_task_id'
application_id = 'test_application_id'
release_label = 'test'
job_type = 'test'
client_request_token = 'eac427d0-1c6d-4dfb9a-32423412'
config = {'name': 'test_application_emr_serverless'}

execution_role_arn = 'test_emr_serverless_role_arn'
job_driver = {'test_key': 'test_value'}
configuration_overrides = {'monitoringConfiguration': {'test_key': 'test_value'}}
job_run_id = 'test_job_run_id'

application_id_delete_operator = 'test_emr_serverless_delete_application_operator'


class TestEmrServerlessCreateApplicationOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_execute_successfully_with_wait_for_completion(self, mock_conn):
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_application.side_effect = [
            {'application': {'state': 'CREATED'}},
            {'application': {'state': 'STARTED'}},
        ]

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        id = operator.execute(None)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.start_application.assert_called_once_with(applicationId=application_id)
        assert id == application_id
        mock_conn.get_application.call_count == 2

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_execute_successfully_no_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            wait_for_completion=False,
            config=config,
        )

        id = operator.execute(None)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.start_application.assert_called_once_with(applicationId=application_id)

        mock_waiter.assert_called_once()
        assert id == application_id

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_create_application_request(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application Creation failed:" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_create_application(self, mock_conn):
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_application.return_value = {'application': {'state': 'TERMINATED'}}

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application reached failure state" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_start_application(self, mock_conn):
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_application.side_effect = [
            {'application': {'state': 'CREATED'}},
            {'application': {'state': 'TERMINATED'}},
        ]

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application reached failure state" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.get_application.call_count == 2

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_no_client_request_token(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            wait_for_completion=False,
            config=config,
        )

        operator.execute(None)
        generated_client_token = operator.client_request_token

        assert str(UUID(generated_client_token, version=4)) == generated_client_token

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_application_in_failure_state(self, mock_conn):
        fail_state = "STOPPED"
        mock_conn.get_application.return_value = {"application": {"state": fail_state}}
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert str(ex_message.value) == f"Application reached failure state {fail_state}."

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )


class TestEmrServerlessStartJobOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_app_started(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        mock_conn.get_job_run.return_value = {'jobRun': {'state': 'SUCCESS'}}

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )

        id = operator.execute(None)

        assert operator.wait_for_completion is True
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )
        mock_conn.get_job_run.assert_called_once_with(applicationId=application_id, jobRunId=job_run_id)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_job_failed(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        mock_conn.get_job_run.return_value = {'jobRun': {'state': 'FAILED'}}

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        with pytest.raises(AirflowException) as ex_message:
            id = operator.execute(None)
            assert id == job_run_id
        assert "Job reached failure state FAILED." in str(ex_message.value)
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_conn.get_job_run.assert_called_once_with(applicationId=application_id, jobRunId=job_run_id)
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_app_not_started(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )

        id = operator.execute(None)

        assert operator.wait_for_completion is True
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        assert mock_waiter.call_count == 2
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_app_not_started_app_failed(self, mock_conn):
        mock_conn.get_application.side_effect = [
            {"application": {"state": "CREATING"}},
            {"application": {"state": "TERMINATED"}},
        ]
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)
        assert "Application reached failure state" in str(ex_message.value)
        assert operator.wait_for_completion is True
        mock_conn.get_application.call_count == 2
        mock_conn.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_app_not_started_no_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            wait_for_completion=False,
        )

        id = operator.execute(None)

        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_waiter.assert_called_once()
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_job_run_app_started_no_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            wait_for_completion=False,
        )

        id = operator.execute(None)
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )
        assert not mock_waiter.called

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_failed_start_job_run(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 404},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "EMR serverless job failed to start:" in str(ex_message.value)
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_waiter.assert_called_once()
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_start_job_run_fail_on_wait_for_completion(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "CREATED"}}
        mock_conn.start_job_run.return_value = {
            'jobRunId': job_run_id,
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        mock_conn.get_job_run.return_value = {'jobRun': {'state': 'FAILED'}}

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Job reached failure state" in str(ex_message.value)
        mock_conn.get_application.call_count == 2
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
        )


class TestEmrServerlessDeleteOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_with_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id, application_id=application_id_delete_operator
        )

        operator.execute(None)

        assert operator.wait_for_completion is True
        assert mock_waiter.call_count == 2
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(applicationId=application_id_delete_operator)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_without_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id,
            application_id=application_id_delete_operator,
            wait_for_completion=False,
        )

        operator.execute(None)

        assert operator.wait_for_completion is False
        mock_waiter.assert_called_once()
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(applicationId=application_id_delete_operator)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_failed_deleteion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 400}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id, application_id=application_id_delete_operator
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application deletion failed:" in str(ex_message.value)

        assert operator.wait_for_completion is True
        mock_waiter.assert_called_once()
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(applicationId=application_id_delete_operator)
