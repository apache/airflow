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

from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest
from boto3 import client
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.operators.kinesis_analytics import (
    KinesisAnalyticsV2CreateApplicationOperator,
    KinesisAnalyticsV2StartApplicationOperator,
    KinesisAnalyticsV2StopApplicationOperator,
)
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection


class TestKinesisAnalyticsV2CreateApplicationOperator:
    APPLICATION_ARN = "arn:aws:kinesisanalytics:us-east-1:918313644466:application/demo"
    ROLE_ARN = "arn:aws:iam::123456789012:role/KinesisExecutionRole"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(KinesisAnalyticsV2Hook, "conn") as _conn:
            _conn.create_application.return_value = {
                "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
            }
            yield _conn

    @pytest.fixture
    def kinesis_analytics_v2_hook(self) -> Generator[KinesisAnalyticsV2Hook, None, None]:
        with mock_aws():
            hook = KinesisAnalyticsV2Hook(aws_conn_id="aws_default")
            yield hook

    def test_init(self):
        op = KinesisAnalyticsV2CreateApplicationOperator(
            task_id="create_application_operator",
            application_name="demo",
            runtime_environment="FLINK_18_9",
            service_execution_role=self.ROLE_ARN,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        assert op.application_name == "demo"
        assert op.runtime_environment == "FLINK_18_9"
        assert op.service_execution_role == self.ROLE_ARN
        assert op.hook.client_type == "kinesisanalyticsv2"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-2"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = KinesisAnalyticsV2CreateApplicationOperator(
            task_id="create_application_operator",
            application_name="demo",
            runtime_environment="FLINK_18_9",
            service_execution_role="arn",
        )

        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_create_application(self, kinesis_analytics_mock_conn):
        self.op = KinesisAnalyticsV2CreateApplicationOperator(
            task_id="create_application_operator",
            application_name="demo",
            application_description="demo app",
            runtime_environment="FLINK_18_9",
            service_execution_role=self.ROLE_ARN,
            create_application_kwargs={
                "ApplicationConfiguration": {
                    "FlinkApplicationConfiguration": {
                        "ParallelismConfiguration": {
                            "ConfigurationType": "CUSTOM",
                            "Parallelism": 2,
                            "ParallelismPerKPU": 1,
                            "AutoScalingEnabled": True,
                        }
                    }
                }
            },
        )
        self.op.execute({})
        kinesis_analytics_mock_conn.create_application.assert_called_once_with(
            ApplicationName="demo",
            ApplicationDescription="demo app",
            RuntimeEnvironment="FLINK_18_9",
            ServiceExecutionRole=self.ROLE_ARN,
            ApplicationConfiguration={
                "FlinkApplicationConfiguration": {
                    "ParallelismConfiguration": {
                        "ConfigurationType": "CUSTOM",
                        "Parallelism": 2,
                        "ParallelismPerKPU": 1,
                        "AutoScalingEnabled": True,
                    }
                }
            },
        )

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_create_application_throw_error_when_invalid_arguments_provided(
        self, kinesis_analytics_mock_conn
    ):
        operator = KinesisAnalyticsV2CreateApplicationOperator(
            task_id="create_application_operator",
            application_name="demo",
            runtime_environment="FLINK_18_9",
            service_execution_role=self.ROLE_ARN,
            create_application_kwargs={"AppId": {"code": "s3://test/flink.jar"}},
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        operator.defer = mock.MagicMock()
        error_message = "Invalid arguments provided"

        err_response = {"Error": {"Code": "InvalidArgumentException", "Message": error_message}}

        exception = client("kinesisanalyticsv2").exceptions.ClientError(
            err_response, operation_name="CreateApplication"
        )
        returned_exception = type(exception)

        kinesis_analytics_mock_conn.exceptions.InvalidArgumentException = returned_exception
        kinesis_analytics_mock_conn.create_application.side_effect = exception

        with pytest.raises(AirflowException, match=error_message):
            operator.execute({})

    def test_template_fields(self):
        operator = KinesisAnalyticsV2CreateApplicationOperator(
            task_id="create_application_operator",
            application_name="demo",
            runtime_environment="FLINK_18_9",
            service_execution_role="arn",
        )
        validate_template_fields(operator)


class TestKinesisAnalyticsV2StartApplicationOperator:
    APPLICATION_ARN = "arn:aws:kinesisanalytics:us-east-1:123456789012:application/demo"
    ROLE_ARN = "arn:aws:iam::123456789012:role/KinesisExecutionRole"
    RUN_CONFIGURATION = {"FlinkRunConfiguration": {"AllowNonRestoredState": True}}

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(KinesisAnalyticsV2Hook, "conn") as _conn:
            _conn.start_application.return_value = {}
            _conn.describe_application.return_value = {
                "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
            }
            yield _conn

    @pytest.fixture
    def kinesis_analytics_v2_hook(self) -> Generator[KinesisAnalyticsV2Hook, None, None]:
        with mock_aws():
            hook = KinesisAnalyticsV2Hook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = KinesisAnalyticsV2StartApplicationOperator(
            task_id="start_application_operator",
            application_name="demo",
            run_configuration=self.RUN_CONFIGURATION,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        self.operator.defer = mock.MagicMock()

    def test_init(self):
        op = KinesisAnalyticsV2StartApplicationOperator(
            task_id="start_application_operator",
            application_name="demo",
            run_configuration=self.RUN_CONFIGURATION,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        assert op.application_name == "demo"
        assert op.run_configuration == self.RUN_CONFIGURATION
        assert op.hook.client_type == "kinesisanalyticsv2"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-2"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = KinesisAnalyticsV2StartApplicationOperator(
            task_id="start_application_operator",
            application_name="demo",
            run_configuration=self.RUN_CONFIGURATION,
        )

        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_start_application(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }
        kinesis_analytics_mock_conn.start_application.return_value = {}

        self.op = KinesisAnalyticsV2StartApplicationOperator(
            task_id="start_application_operator",
            application_name="demo",
            run_configuration=self.RUN_CONFIGURATION,
        )
        self.op.wait_for_completion = False
        response = self.op.execute({})

        assert response == {"ApplicationARN": self.APPLICATION_ARN}

        kinesis_analytics_mock_conn.start_application.assert_called_once_with(
            ApplicationName="demo", RunConfiguration=self.RUN_CONFIGURATION
        )

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(KinesisAnalyticsV2Hook, "get_waiter")
    def test_start_application_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, kinesis_analytics_v2_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == {"ApplicationARN": self.APPLICATION_ARN}
        assert kinesis_analytics_v2_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_start_application_throw_error_when_invalid_config_provided(self, kinesis_analytics_mock_conn):
        operator = KinesisAnalyticsV2StartApplicationOperator(
            task_id="start_application_operator",
            application_name="demo",
            run_configuration={
                "ApplicationRestoreConfiguration": {
                    "ApplicationRestoreType": "SKIP_RESTORE",
                }
            },
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        operator.defer = mock.MagicMock()
        error_message = "Invalid config provided"

        err_response = {
            "Error": {"Code": "InvalidApplicationConfigurationException", "Message": error_message}
        }

        exception = client("kinesisanalyticsv2").exceptions.ClientError(
            err_response, operation_name="StartApplication"
        )
        returned_exception = type(exception)

        kinesis_analytics_mock_conn.exceptions.InvalidArgumentException = returned_exception
        kinesis_analytics_mock_conn.start_application.side_effect = exception

        with pytest.raises(AirflowException, match=error_message):
            operator.execute({})

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_execute_complete(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }

        event = {"status": "success", "application_name": "demo"}

        response = self.operator.execute_complete(context=None, event=event)

        assert {"ApplicationARN": self.APPLICATION_ARN} == response

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_execute_complete_failure(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }

        event = {"status": "error", "application_name": "demo"}

        with pytest.raises(
            AirflowException,
            match="Error while starting AWS Managed Service for Apache Flink application",
        ):
            self.operator.execute_complete(context=None, event=event)

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestKinesisAnalyticsV2StopApplicationOperator:
    APPLICATION_ARN = "arn:aws:kinesisanalytics:us-east-1:123456789012:application/demo"
    ROLE_ARN = "arn:aws:iam::123456789012:role/KinesisExecutionRole"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(KinesisAnalyticsV2Hook, "conn") as _conn:
            _conn.stop_application.return_value = {}
            _conn.describe_application.return_value = {
                "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
            }
            yield _conn

    @pytest.fixture
    def kinesis_analytics_v2_hook(self) -> Generator[KinesisAnalyticsV2Hook, None, None]:
        with mock_aws():
            hook = KinesisAnalyticsV2Hook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = KinesisAnalyticsV2StopApplicationOperator(
            task_id="stop_application_operator",
            application_name="demo",
            force=False,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        self.operator.defer = mock.MagicMock()

    def test_init(self):
        op = KinesisAnalyticsV2StopApplicationOperator(
            task_id="stop_application_operator",
            application_name="demo",
            force=False,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        assert op.application_name == "demo"
        assert op.force is False
        assert op.hook.client_type == "kinesisanalyticsv2"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-2"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = KinesisAnalyticsV2StopApplicationOperator(
            task_id="stop_application_operator",
            application_name="demo",
            force=False,
        )

        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_stop_application(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }
        kinesis_analytics_mock_conn.stop_application.return_value = {}

        self.op = KinesisAnalyticsV2StopApplicationOperator(
            task_id="stop_application_operator", application_name="demo", force=False
        )
        self.op.wait_for_completion = False
        response = self.op.execute({})

        assert response == {"ApplicationARN": self.APPLICATION_ARN}

        kinesis_analytics_mock_conn.stop_application.assert_called_once_with(
            ApplicationName="demo", Force=False
        )

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(KinesisAnalyticsV2Hook, "get_waiter")
    def test_stop_application_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, kinesis_analytics_v2_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == {"ApplicationARN": self.APPLICATION_ARN}
        assert kinesis_analytics_v2_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_stop_application_throw_error_when_invalid_config_provided(self, kinesis_analytics_mock_conn):
        operator = KinesisAnalyticsV2StopApplicationOperator(
            task_id="stop_application_operator",
            application_name="demo",
            force=False,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        operator.defer = mock.MagicMock()
        error_message = "resource not found"

        err_response = {"Error": {"Code": "ResourceNotFoundException", "Message": error_message}}

        exception = client("kinesisanalyticsv2").exceptions.ClientError(
            err_response, operation_name="StopApplication"
        )
        returned_exception = type(exception)

        kinesis_analytics_mock_conn.exceptions.ResourceNotFoundException = returned_exception
        kinesis_analytics_mock_conn.stop_application.side_effect = exception

        with pytest.raises(AirflowException, match=error_message):
            operator.execute({})

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_execute_complete(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }

        event = {"status": "success", "application_name": "demo"}

        response = self.operator.execute_complete(context=None, event=event)

        assert {"ApplicationARN": self.APPLICATION_ARN} == response

    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_execute_complete_failure(self, kinesis_analytics_mock_conn):
        kinesis_analytics_mock_conn.describe_application.return_value = {
            "ApplicationDetail": {"ApplicationARN": self.APPLICATION_ARN}
        }
        event = {"status": "error", "application_name": "demo"}

        with pytest.raises(
            AirflowException, match="Error while stopping AWS Managed Service for Apache Flink application"
        ):
            self.operator.execute_complete(context=None, event=event)

    def test_template_fields(self):
        validate_template_fields(self.operator)
