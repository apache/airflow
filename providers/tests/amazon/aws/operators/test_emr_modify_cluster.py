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

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import EmrModifyClusterOperator
from airflow.utils import timezone

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MODIFY_CLUSTER_SUCCESS_RETURN = {
    "ResponseMetadata": {"HTTPStatusCode": 200},
    "StepConcurrencyLevel": 1,
}
MODIFY_CLUSTER_ERROR_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 400}}


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn") as m:
        yield m


class TestEmrModifyClusterOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.mock_context = MagicMock()
        self.operator = EmrModifyClusterOperator(
            task_id="test_task",
            cluster_id="j-8989898989",
            step_concurrency_level=1,
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", schedule=None, default_args=args),
        )

    def test_init(self):
        assert self.operator.cluster_id == "j-8989898989"
        assert self.operator.step_concurrency_level == 1
        assert self.operator.aws_conn_id == "aws_default"

    def test_execute_returns_step_concurrency(self, mocked_hook_client):
        mocked_hook_client.modify_cluster.return_value = MODIFY_CLUSTER_SUCCESS_RETURN

        assert self.operator.execute(self.mock_context) == 1

    def test_execute_returns_error(self, mocked_hook_client):
        mocked_hook_client.modify_cluster.return_value = MODIFY_CLUSTER_ERROR_RETURN

        with pytest.raises(AirflowException, match="Modify cluster failed"):
            self.operator.execute(self.mock_context)

    def test_template_fields(self):
        validate_template_fields(self.operator)
