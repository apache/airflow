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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.triggers.emr import EmrTerminateJobFlowTrigger

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

TERMINATE_SUCCESS_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn") as m:
        yield m


class TestEmrTerminateJobFlowOperator:
    def test_execute_terminates_the_job_flow_and_does_not_error(self, mocked_hook_client):
        mocked_hook_client.terminate_job_flows.return_value = TERMINATE_SUCCESS_RETURN
        operator = EmrTerminateJobFlowOperator(
            task_id="test_task", job_flow_id="j-8989898989", aws_conn_id="aws_default"
        )

        operator.execute(MagicMock())

    def test_create_job_flow_deferrable(self, mocked_hook_client):
        mocked_hook_client.terminate_job_flows.return_value = TERMINATE_SUCCESS_RETURN
        operator = EmrTerminateJobFlowOperator(
            task_id="test_task",
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(MagicMock())
        assert isinstance(
            exc.value.trigger, EmrTerminateJobFlowTrigger
        ), "Trigger is not a EmrTerminateJobFlowTrigger"

    def test_template_fields(self):
        operator = EmrTerminateJobFlowOperator(
            task_id="test_task",
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            deferrable=True,
        )

        validate_template_fields(operator)
