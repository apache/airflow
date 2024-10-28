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

from typing import TYPE_CHECKING
from unittest import mock
from uuid import UUID

import pytest
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
)
from airflow.utils.types import NOTSET

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from unittest.mock import MagicMock

task_id = "test_emr_serverless_task_id"
application_id = "test_application_id"
release_label = "test"
job_type = "test"
client_request_token = "eac427d0-1c6d-4dfb9a-32423412"
config = {"name": "test_application_emr_serverless"}

execution_role_arn = "test_emr_serverless_role_arn"
job_driver = {"test_key": "test_value"}
spark_job_driver = {"sparkSubmit": {"entryPoint": "test.py"}}
configuration_overrides = {"monitoringConfiguration": {"test_key": "test_value"}}
job_run_id = "test_job_run_id"
s3_logs_location = "s3://test_bucket/test_key/"
cloudwatch_logs_group_name = "/aws/emrs"
cloudwatch_logs_prefix = "myapp"
s3_configuration_overrides = {
    "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": s3_logs_location}}
}
cloudwatch_configuration_overrides = {
    "monitoringConfiguration": {
        "cloudWatchLoggingConfiguration": {
            "enabled": True,
            "logGroupName": cloudwatch_logs_group_name,
            "logStreamNamePrefix": cloudwatch_logs_prefix,
        }
    }
}

application_id_delete_operator = "test_emr_serverless_delete_application_operator"


class TestEmrServerlessCreateApplicationOperator:
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_execute_successfully_with_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter().wait.return_value = True
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_application.side_effect = [
            {"application": {"state": "CREATED"}},
            {"application": {"state": "STARTED"}},
        ]

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
            waiter_max_attempts=3,
            waiter_delay=0,
        )

        id = operator.execute(None)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_waiter().wait.assert_called_with(
            applicationId=application_id,
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter().wait.call_count == 2

        mock_conn.start_application.assert_called_once_with(applicationId=application_id)
        assert id == application_id
        mock_conn.get_application.call_count == 2

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_execute_successfully_no_wait_for_completion(self, mock_conn, mock_waiter):
        mock_waiter().wait.return_value = True
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

        mock_waiter().wait.assert_called_once()
        assert id == application_id

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_failed_create_application_request(self, mock_conn):
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

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_failed_create_application(self, mock_conn, mock_get_waiter):
        error = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"application": {"state": "FAILED"}},
        )
        mock_get_waiter().wait.side_effect = error
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

        assert "Serverless Application creation failed:" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.create_application.return_value = {
            "applicationId": application_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        error = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"application": {"state": "TERMINATED"}},
        )
        mock_get_waiter().wait.side_effect = error

        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
        )

        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Serverless Application creation failed:" in str(ex_message.value)

        mock_conn.create_application.assert_called_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )
        mock_conn.create_application.call_count == 2

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_failed_start_application(self, mock_conn, mock_get_waiter):
        error = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"application": {"state": "TERMINATED"}},
        )
        mock_get_waiter().wait.side_effect = [True, error]
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

        assert "Serverless Application failed to start:" in str(ex_message.value)

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_no_client_request_token(self, mock_conn, mock_waiter):
        mock_waiter().wait.return_value = True
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

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_application_in_failure_state(self, mock_conn, mock_get_waiter):
        fail_state = "STOPPED"
        error = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"application": {"state": fail_state}},
        )
        mock_get_waiter().wait.side_effect = [error]
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

        assert str(ex_message.value) == f"Serverless Application creation failed: {error}"

        mock_conn.create_application.assert_called_once_with(
            clientToken=client_request_token,
            releaseLabel=release_label,
            type=job_type,
            **config,
        )

    @pytest.mark.parametrize(
        "waiter_delay, waiter_max_attempts, expected",
        [
            (NOTSET, NOTSET, [60, 25]),
            (30, 10, [30, 10]),
        ],
    )
    def test_create_application_waiter_params(
        self,
        waiter_delay,
        waiter_max_attempts,
        expected,
    ):
        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == expected[0]
        assert operator.waiter_max_attempts == expected[1]

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_create_application_deferrable(self, mock_conn):
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
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            operator.execute(None)

    def test_template_fields(self):
        operator = EmrServerlessCreateApplicationOperator(
            task_id=task_id,
            release_label=release_label,
            job_type=job_type,
            client_request_token=client_request_token,
            config=config,
            waiter_max_attempts=3,
            waiter_delay=0,
        )

        template_fields = list(operator.template_fields) + list(
            operator.template_fields_renderers.keys()
        )

        class_fields = operator.__dict__

        missing_fields = [field for field in template_fields if field not in class_fields]

        assert not missing_fields, f"Templated fields are not available {missing_fields}"


class TestEmrServerlessStartJobOperator:
    def setup_method(self):
        self.mock_context = mock.MagicMock()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_app_started(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        id = operator.execute(self.mock_context)
        default_name = operator.name

        assert operator.wait_for_completion is True
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_job_failed(self, mock_conn, mock_get_waiter):
        error = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"jobRun": {"state": "FAILED"}},
        )
        mock_get_waiter().wait.side_effect = [error]
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
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
            operator.execute(self.mock_context)

        assert "Serverless Job failed:" in str(ex_message.value)
        default_name = operator.name

        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_app_not_started(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        id = operator.execute(self.mock_context)
        default_name = operator.name

        assert operator.wait_for_completion is True
        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        assert mock_get_waiter().wait.call_count == 2
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )

    @mock.patch("time.sleep", return_value=True)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_app_not_started_app_failed(
        self, mock_conn, mock_get_waiter, mock_time
    ):
        error1 = WaiterError(
            name="test_name",
            reason="test-reason",
            last_response={
                "application": {"state": "CREATING", "stateDetails": "test-details"}
            },
        )
        error2 = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={
                "application": {"state": "TERMINATED", "stateDetails": "test-details"}
            },
        )
        mock_get_waiter().wait.side_effect = [error1, error2]
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
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
            operator.execute(self.mock_context)
        assert "Serverless Application failed to start:" in str(ex_message.value)
        assert operator.wait_for_completion is True
        assert mock_get_waiter().wait.call_count == 2

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_app_not_started_no_wait_for_completion(
        self, mock_conn, mock_get_waiter
    ):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
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
        id = operator.execute(self.mock_context)
        default_name = operator.name

        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_get_waiter().wait.assert_called_once()
        assert id == job_run_id
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_job_run_app_started_no_wait_for_completion(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
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
        id = operator.execute(self.mock_context)
        assert id == job_run_id
        default_name = operator.name

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )
        assert not mock_get_waiter().wait.called

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_failed_start_job_run(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 404},
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
            operator.execute(self.mock_context)
        assert "EMR serverless job failed to start:" in str(ex_message.value)
        default_name = operator.name

        mock_conn.get_application.assert_called_once_with(applicationId=application_id)
        mock_get_waiter().wait.assert_called_once()
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_run_fail_on_wait_for_completion(self, mock_conn, mock_get_waiter):
        error = WaiterError(
            name="mock_waiter_error",
            reason="Waiter encountered a terminal failure state:",
            last_response={"jobRun": {"state": "FAILED", "stateDetails": "Test Details"}},
        )
        mock_get_waiter().wait.side_effect = [error]
        mock_conn.get_application.return_value = {"application": {"state": "CREATED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
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
            operator.execute(self.mock_context)
        assert "Serverless Job failed:" in str(ex_message.value)
        default_name = operator.name

        mock_conn.get_application.call_count == 2
        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=default_name,
        )
        mock_get_waiter().wait.assert_called_once()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_default_name(self, mock_conn, mock_get_waiter):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_get_waiter().wait.return_value = True

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        operator.execute(self.mock_context)
        default_name = operator.name
        generated_name_uuid = default_name.split("_")[-1]
        assert default_name.startswith("emr_serverless_job_airflow")

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=f"emr_serverless_job_airflow_{UUID(generated_name_uuid, version=4)}",
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_custom_name(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        custom_name = "test_name"
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            name=custom_name,
        )
        operator.execute(self.mock_context)

        mock_conn.start_job_run.assert_called_once_with(
            clientToken=client_request_token,
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides,
            name=custom_name,
        )

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_cancel_job_run(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_conn.get_job_run.return_value = {"jobRun": {"state": "RUNNING"}}

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            wait_for_completion=False,
        )

        id = operator.execute(self.mock_context)
        operator.on_kill()
        mock_conn.cancel_job_run.assert_called_once_with(
            applicationId=application_id,
            jobRunId=id,
        )

    @pytest.mark.parametrize(
        "waiter_delay, waiter_max_attempts, expected",
        [
            (NOTSET, NOTSET, [60, 25]),
            (30, 10, [30, 10]),
        ],
    )
    def test_start_job_waiter_params(
        self,
        waiter_delay,
        waiter_max_attempts,
        expected,
    ):
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == expected[0]
        assert operator.waiter_max_attempts == expected[1]

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_deferrable(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            operator.execute(self.mock_context)

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_deferrable_without_wait_for_completion(self, mock_conn):
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            deferrable=True,
            wait_for_completion=False,
        )

        result = operator.execute(self.mock_context)

        assert result == job_run_id

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_start_job_deferrable_app_not_started(self, mock_conn, mock_get_waiter):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "CREATING"}}
        mock_conn.start_application.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            operator.execute(self.mock_context)

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_start_job_default(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_s3_logs_link.assert_not_called()
        mock_logs_link.assert_not_called()
        mock_dashboard_link.assert_not_called()
        mock_cloudwatch_link.assert_not_called()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_s3_enabled(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=s3_configuration_overrides,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_logs_link.assert_not_called()
        mock_dashboard_link.assert_not_called()
        mock_cloudwatch_link.assert_not_called()
        mock_s3_logs_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            log_uri=s3_logs_location,
            application_id=application_id,
            job_run_id=job_run_id,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_cloudwatch_enabled(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=cloudwatch_configuration_overrides,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_logs_link.assert_not_called()
        mock_dashboard_link.assert_not_called()
        mock_s3_logs_link.assert_not_called()
        mock_cloudwatch_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            awslogs_group=cloudwatch_logs_group_name,
            stream_prefix=f"{cloudwatch_logs_prefix}/applications/{application_id}/jobs/{job_run_id}",
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_applicationui_enabled(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=cloudwatch_configuration_overrides,
            enable_application_ui_links=True,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_logs_link.assert_not_called()
        mock_s3_logs_link.assert_not_called()
        mock_dashboard_link.assert_called_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            conn_id=mock.ANY,
            application_id=application_id,
            job_run_id=job_run_id,
        )
        mock_cloudwatch_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            awslogs_group=cloudwatch_logs_group_name,
            stream_prefix=f"{cloudwatch_logs_prefix}/applications/{application_id}/jobs/{job_run_id}",
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_applicationui_with_spark_enabled(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=spark_job_driver,
            configuration_overrides=s3_configuration_overrides,
            enable_application_ui_links=True,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_logs_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            conn_id=mock.ANY,
            application_id=application_id,
            job_run_id=job_run_id,
        )
        mock_dashboard_link.assert_called_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            conn_id=mock.ANY,
            application_id=application_id,
            job_run_id=job_run_id,
        )
        mock_cloudwatch_link.assert_not_called()
        mock_s3_logs_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            log_uri=s3_logs_location,
            application_id=application_id,
            job_run_id=job_run_id,
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessCloudWatchLogsLink.persist"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.links.emr.EmrServerlessDashboardLink.persist"
    )
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessLogsLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink.persist")
    def test_links_spark_without_applicationui_enabled(
        self,
        mock_s3_logs_link,
        mock_logs_link,
        mock_dashboard_link,
        mock_cloudwatch_link,
        mock_conn,
        mock_get_waiter,
    ):
        mock_get_waiter.wait.return_value = True
        mock_conn.get_application.return_value = {"application": {"state": "STARTED"}}
        mock_conn.start_job_run.return_value = {
            "jobRunId": job_run_id,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=spark_job_driver,
            configuration_overrides=s3_configuration_overrides,
            enable_application_ui_links=False,
        )
        operator.execute(self.mock_context)
        mock_conn.start_job_run.assert_called_once()

        mock_logs_link.assert_not_called()
        mock_dashboard_link.assert_not_called()
        mock_cloudwatch_link.assert_not_called()
        mock_s3_logs_link.assert_called_once_with(
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            aws_partition=mock.ANY,
            log_uri=s3_logs_location,
            application_id=application_id,
            job_run_id=job_run_id,
        )

    def test_template_fields(self):
        operator = EmrServerlessStartJobOperator(
            task_id=task_id,
            client_request_token=client_request_token,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
            job_driver=job_driver,
            configuration_overrides=configuration_overrides,
        )

        template_fields = list(operator.template_fields) + list(
            operator.template_fields_renderers.keys()
        )

        class_fields = operator.__dict__

        missing_fields = [field for field in template_fields if field not in class_fields]

        assert not missing_fields, f"Templated fields are not available {missing_fields}"


class TestEmrServerlessDeleteOperator:
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_delete_application_with_wait_for_completion_successfully(
        self, mock_conn, mock_get_waiter
    ):
        mock_get_waiter().wait.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id, application_id=application_id_delete_operator
        )

        operator.execute(None)

        assert operator.wait_for_completion is True
        assert mock_get_waiter().wait.call_count == 2
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(
            applicationId=application_id_delete_operator
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_delete_application_without_wait_for_completion_successfully(
        self, mock_conn, mock_get_waiter
    ):
        mock_get_waiter().wait.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id,
            application_id=application_id_delete_operator,
            wait_for_completion=False,
        )

        operator.execute(None)

        mock_get_waiter().wait.assert_called_once()
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(
            applicationId=application_id_delete_operator
        )

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_delete_application_failed_deletion(self, mock_conn, mock_get_waiter):
        mock_get_waiter().wait.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id, application_id=application_id_delete_operator
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application deletion failed:" in str(ex_message.value)

        mock_get_waiter().wait.assert_called_once()
        mock_conn.stop_application.assert_called_once()
        mock_conn.delete_application.assert_called_once_with(
            applicationId=application_id_delete_operator
        )

    @pytest.mark.parametrize(
        "waiter_delay, waiter_max_attempts, expected",
        [
            (NOTSET, NOTSET, [60, 25]),
            (30, 10, [30, 10]),
        ],
    )
    def test_delete_application_waiter_params(
        self,
        waiter_delay,
        waiter_max_attempts,
        expected,
    ):
        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id,
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == expected[0]
        assert operator.waiter_max_attempts == expected[1]

    @mock.patch.object(EmrServerlessHook, "conn")
    def test_delete_application_deferrable(self, mock_conn):
        mock_conn.delete_application.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id,
            application_id=application_id,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            operator.execute(None)

    def test_template_fields(self):
        operator = EmrServerlessDeleteApplicationOperator(
            task_id=task_id, application_id=application_id_delete_operator
        )

        template_fields = list(operator.template_fields) + list(
            operator.template_fields_renderers.keys()
        )

        class_fields = operator.__dict__

        missing_fields = [field for field in template_fields if field not in class_fields]

        assert not missing_fields, f"Templated fields are not available {missing_fields}"


class TestEmrServerlessStopOperator:
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_stop(self, mock_conn: MagicMock, mock_get_waiter: MagicMock):
        mock_get_waiter().wait.return_value = True
        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test"
        )

        operator.execute({})

        mock_get_waiter().wait.assert_called_once()
        mock_conn.stop_application.assert_called_once()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    def test_stop_no_wait(self, mock_conn: MagicMock, mock_get_waiter: MagicMock):
        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test", wait_for_completion=False
        )

        operator.execute({})

        mock_get_waiter().wait.assert_not_called()
        mock_conn.stop_application.assert_called_once()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch.object(EmrServerlessHook, "cancel_running_jobs")
    def test_force_stop(self, mock_cancel_running_jobs, mock_conn, mock_get_waiter):
        mock_cancel_running_jobs.return_value = 0
        mock_conn.stop_application.return_value = {}
        mock_get_waiter().wait.return_value = True

        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test", force_stop=True
        )

        operator.execute({})

        mock_cancel_running_jobs.assert_called_once()
        mock_conn.stop_application.assert_called_once()
        mock_get_waiter().wait.assert_called_once()

    @mock.patch.object(EmrServerlessHook, "cancel_running_jobs")
    def test_stop_application_deferrable_with_force_stop(
        self, mock_cancel_running_jobs, caplog
    ):
        mock_cancel_running_jobs.return_value = 2
        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test", deferrable=True, force_stop=True
        )
        with pytest.raises(TaskDeferred):
            operator.execute({})
        assert "now waiting for the 2 cancelled job(s) to terminate" in caplog.messages

    @mock.patch.object(EmrServerlessHook, "conn")
    @mock.patch.object(EmrServerlessHook, "cancel_running_jobs")
    def test_stop_application_deferrable_without_force_stop(
        self, mock_cancel_running_jobs, mock_conn, caplog
    ):
        mock_conn.stop_application.return_value = {}
        mock_cancel_running_jobs.return_value = 0
        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test", deferrable=True, force_stop=True
        )
        with pytest.raises(TaskDeferred):
            operator.execute({})

        assert "no running jobs found with application ID test" in caplog.messages

    def test_template_fields(self):
        operator = EmrServerlessStopApplicationOperator(
            task_id=task_id, application_id="test", deferrable=True, force_stop=True
        )

        validate_template_fields(operator)
