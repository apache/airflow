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
from moto import mock_databrew

from airflow.providers.amazon.aws.hooks.glue import GlueDataBrewHook
from airflow.providers.amazon.aws.operators.glue import GlueDataBrewStartJobOperator

JOB_NAME = "test_job"


@pytest.fixture
def hook() -> GlueDataBrewHook:
    with mock_databrew():
        yield GlueDataBrewHook(aws_conn_id="aws_default")


class TestGlueDataBrewOperator:
    @mock.patch.object(GlueDataBrewHook, "get_waiter")
    def test_start_job_wait_for_completion(self, mock_hook_get_waiter):
        operator = GlueDataBrewStartJobOperator(
            task_id="task_test", job_name=JOB_NAME, wait_for_completion=True
        )
        operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("databrew_job_complete")
