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

from airflow.exceptions import TaskDeferred
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2018, 1, 1)
ATHENA_QUERY_ID = "eac29bf8-daa1-4ffc-b19a-0db31dc3b784"

MOCK_DATA = {
    "task_id": "test_athena_operator",
    "query": "SELECT * FROM TEST_TABLE",
    "database": "TEST_DATABASE",
    "catalog": "AwsDataCatalog",
    "outputLocation": "s3://test_s3_bucket/",
    "client_request_token": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "workgroup": "primary",
}

query_context = {"Database": MOCK_DATA["database"], "Catalog": MOCK_DATA["catalog"]}
result_configuration = {"OutputLocation": MOCK_DATA["outputLocation"]}


class TestAthenaOperator:
    def setup_method(self):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(TEST_DAG_ID, default_args=args, schedule="@once")

        self.default_op_kwargs = dict(
            task_id="test_athena_operator",
            query="SELECT * FROM TEST_TABLE",
            database="TEST_DATABASE",
            client_request_token="eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
            sleep_time=0,
            max_polling_attempts=3,
        )
        self.athena = AthenaOperator(
            **self.default_op_kwargs, output_location="s3://test_s3_bucket/", aws_conn_id=None, dag=self.dag
        )

    def test_base_aws_op_attributes(self):
        op = AthenaOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.hook.log_query is True

        op = AthenaOperator(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
            log_query=False,
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42
        assert op.hook.log_query is False

    def test_init(self):
        assert self.athena.task_id == MOCK_DATA["task_id"]
        assert self.athena.query == MOCK_DATA["query"]
        assert self.athena.database == MOCK_DATA["database"]
        assert self.athena.catalog == MOCK_DATA["catalog"]
        assert self.athena.client_request_token == MOCK_DATA["client_request_token"]
        assert self.athena.sleep_time == 0

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_override_catalog(self, mock_conn, mock_run_query, mock_check_query_status):
        query_context_catalog = {"Database": MOCK_DATA["database"], "Catalog": "MyCatalog"}
        self.athena.catalog = "MyCatalog"
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context_catalog,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )
        assert mock_check_query_status.call_count == 1

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_small_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )
        assert mock_check_query_status.call_count == 1

    @mock.patch.object(
        AthenaHook,
        "check_query_status",
        side_effect="SUCCEEDED",
    )
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_big_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "get_state_change_reason")
    @mock.patch.object(AthenaHook, "check_query_status", return_value="FAILED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_failure_query(
        self, mock_conn, mock_run_query, mock_check_query_status, mock_get_state_change_reason
    ):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )
        assert mock_get_state_change_reason.call_count == 1

    @mock.patch.object(AthenaHook, "check_query_status", return_value="CANCELLED")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_cancelled_query(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "check_query_status", return_value="RUNNING")
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_failed_query_with_max_tries(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            result_configuration,
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )

    @pytest.mark.db_test
    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_return_value(self, mock_conn, mock_run_query, mock_check_query_status):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.athena)
        ti.dag_run = dag_run

        assert self.athena.execute(ti.get_template_context()) == ATHENA_QUERY_ID

    @mock.patch.object(AthenaHook, "check_query_status", side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_optional_output_location(self, mock_conn, mock_run_query, mock_check_query_status):
        op = AthenaOperator(**self.default_op_kwargs, aws_conn_id=None)

        op.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"],
            query_context,
            {},  # Should be an empty dict since we do not provide output_location
            MOCK_DATA["client_request_token"],
            MOCK_DATA["workgroup"],
        )

    @mock.patch.object(AthenaHook, "run_query", return_value=ATHENA_QUERY_ID)
    def test_is_deferred(self, mock_run_query):
        self.athena.deferrable = True

        with pytest.raises(TaskDeferred) as deferred:
            self.athena.execute(None)

        assert isinstance(deferred.value.trigger, AthenaTrigger)
