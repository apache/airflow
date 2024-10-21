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

from typing import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.glue_databrew import GlueDataBrewHook
from airflow.providers.amazon.aws.operators.glue_databrew import GlueDataBrewStartJobOperator

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

JOB_NAME = "test_job"


@pytest.fixture
def hook() -> Generator[GlueDataBrewHook, None, None]:
    with mock_aws():
        yield GlueDataBrewHook(aws_conn_id="aws_default")


class TestGlueDataBrewOperator:
    def test_init(self):
        op = GlueDataBrewStartJobOperator(
            task_id="task_test",
            job_name=JOB_NAME,
            aws_conn_id="fake-conn-id",
            region_name="eu-central-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )

        assert op.hook.client_type == "databrew"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-central-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = GlueDataBrewStartJobOperator(task_id="fake_task_id", job_name=JOB_NAME)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(GlueDataBrewHook, "conn")
    @mock.patch.object(GlueDataBrewHook, "get_waiter")
    def test_start_job_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        TEST_RUN_ID = "12345"
        operator = GlueDataBrewStartJobOperator(
            task_id="task_test", job_name=JOB_NAME, wait_for_completion=True, aws_conn_id="aws_default"
        )
        mock_conn.start_job_run(mock.MagicMock(), return_value=TEST_RUN_ID)
        operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("job_complete")

    @mock.patch.object(GlueDataBrewHook, "conn")
    @mock.patch.object(GlueDataBrewHook, "get_waiter")
    def test_start_job_no_wait(self, mock_hook_get_waiter, mock_conn):
        TEST_RUN_ID = "12345"
        operator = GlueDataBrewStartJobOperator(
            task_id="task_test", job_name=JOB_NAME, wait_for_completion=False, aws_conn_id="aws_default"
        )
        mock_conn.start_job_run(mock.MagicMock(), return_value=TEST_RUN_ID)
        operator.execute(None)
        mock_hook_get_waiter.assert_not_called()

    def test_template_fields(self):
        operator = GlueDataBrewStartJobOperator(task_id="fake_task_id", job_name=JOB_NAME)
        validate_template_fields(operator)
