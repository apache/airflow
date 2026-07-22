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

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import QuickSightIngestionFailedError
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.triggers.quicksight import QuickSightIngestionCompletedTrigger
from airflow.providers.common.compat.sdk import TaskDeferred

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

DATA_SET_ID = "DemoDataSet"
INGESTION_ID = "DemoDataSet_Ingestion"
AWS_ACCOUNT_ID = "123456789012"
INGESTION_TYPE = "FULL_REFRESH"

MOCK_RESPONSE = {
    "Status": 201,
    "Arn": "arn:aws:quicksight:us-east-1:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet_Ingestion",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionStatus": "INITIALIZED",
    "RequestId": "fc1f7eea-1327-41d6-9af7-c12f097ed343",
}


@pytest.fixture
def mocked_account_id():
    with mock.patch.object(QuickSightHook, "account_id", new_callable=mock.PropertyMock) as m:
        m.return_value = AWS_ACCOUNT_ID
        yield m


class TestQuickSightCreateIngestionOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "task_id": "quicksight_create",
            "aws_conn_id": None,
            "data_set_id": DATA_SET_ID,
            "ingestion_id": INGESTION_ID,
        }

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)

        op = QuickSightCreateIngestionOperator(
            **self.default_op_kwargs,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="cn-north-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.client_type == "quicksight"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "cn-north-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.wait_for_completion is True
        assert op.waiter_delay == 30
        assert op.waiter_max_attempts == 60
        assert op.deferrable is False

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(QuickSightCreateIngestionOperator, "defer")
    @mock.patch.object(QuickSightHook, "wait_for_ingestion")
    @mock.patch.object(QuickSightHook, "create_ingestion", return_value=MOCK_RESPONSE)
    def test_execute_wait_combinations(
        self,
        mock_create_ingestion,
        mock_wait_for_ingestion,
        mock_defer,
        wait_for_completion,
        deferrable,
        mocked_account_id,
    ):
        op = QuickSightCreateIngestionOperator(
            **self.default_op_kwargs, wait_for_completion=wait_for_completion, deferrable=deferrable
        )

        response = op.execute({})

        assert response == MOCK_RESPONSE
        mock_create_ingestion.assert_called_once_with(
            data_set_id=DATA_SET_ID,
            ingestion_id=INGESTION_ID,
            ingestion_type=INGESTION_TYPE,
            wait_for_completion=False,
        )
        assert mock_wait_for_ingestion.call_count == wait_for_completion
        assert mock_defer.call_count == deferrable

    @mock.patch.object(QuickSightHook, "create_ingestion", return_value=MOCK_RESPONSE)
    def test_execute_deferrable(self, mock_create_ingestion, mocked_account_id):
        op = QuickSightCreateIngestionOperator(
            **self.default_op_kwargs, deferrable=True, waiter_delay=15, waiter_max_attempts=3
        )

        with pytest.raises(TaskDeferred) as defer_exc:
            op.execute({})

        trigger = defer_exc.value.trigger
        assert isinstance(trigger, QuickSightIngestionCompletedTrigger)
        assert defer_exc.value.method_name == "execute_complete"
        assert defer_exc.value.kwargs == {"ingestion": MOCK_RESPONSE}
        _, kwargs = trigger.serialize()
        assert kwargs["data_set_id"] == DATA_SET_ID
        assert kwargs["ingestion_id"] == INGESTION_ID
        assert kwargs["aws_account_id"] == AWS_ACCOUNT_ID
        assert kwargs["waiter_delay"] == 15
        assert kwargs["waiter_max_attempts"] == 3

    def test_execute_complete(self):
        op = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        event = {"status": "success", "ingestion_id": INGESTION_ID}

        assert op.execute_complete({}, event, ingestion=MOCK_RESPONSE) == MOCK_RESPONSE

    def test_execute_complete_failure(self):
        op = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        event = {"status": "error", "message": "test failure", "ingestion_id": INGESTION_ID}

        with pytest.raises(QuickSightIngestionFailedError, match="Error while running"):
            op.execute_complete({}, event)

    @pytest.mark.parametrize("check_interval", [10, "10"], ids=["int", "templated_str"])
    @mock.patch.object(QuickSightHook, "wait_for_ingestion")
    @mock.patch.object(QuickSightHook, "create_ingestion", return_value=MOCK_RESPONSE)
    def test_check_interval_deprecation(self, mock_create_ingestion, mock_wait_for_ingestion, check_interval):
        with pytest.warns(AirflowProviderDeprecationWarning, match="check_interval"):
            op = QuickSightCreateIngestionOperator(**self.default_op_kwargs, check_interval=check_interval)

        op.execute({})

        mock_wait_for_ingestion.assert_called_once_with(
            data_set_id=DATA_SET_ID,
            ingestion_id=INGESTION_ID,
            waiter_delay=10,
            waiter_max_attempts=60,
        )

    def test_template_fields(self):
        operator = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        validate_template_fields(operator)
