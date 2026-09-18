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

from airflow.providers.amazon.aws.hooks.duckdb import AwsDuckDBHook
from airflow.providers.amazon.aws.operators.duckdb import AwsDuckDBOperator

GET_CONNECTION = "airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook.get_connection"

try:
    from airflow.sdk.exceptions import AirflowNotFoundException
except ImportError:
    from airflow.exceptions import AirflowNotFoundException  # type: ignore[no-redef]


class TestAwsDuckDBOperator:
    def test_get_db_hook_returns_the_aws_duckdb_hook(self):
        operator = AwsDuckDBOperator(task_id="t", sql="SELECT 1")
        assert isinstance(operator.get_db_hook(), AwsDuckDBHook)

    def test_no_duckdb_connection_is_required(self):
        operator = AwsDuckDBOperator(task_id="t", sql="SELECT 1", conn_id=AwsDuckDBHook.default_conn_name)
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert operator.get_db_hook().get_database() == ":memory:"

    def test_aws_hook_params_are_forwarded(self):
        operator = AwsDuckDBOperator(
            task_id="t",
            sql="SELECT 1",
            hook_params={
                "aws_conn_id": "aws_prod",
                "region_name": "eu-west-1",
                "credential_strategy": "config",
                "extensions": ["iceberg"],
            },
        )
        hook = operator.get_db_hook()
        assert hook.aws_conn_id == "aws_prod"
        assert hook.region_name == "eu-west-1"
        assert hook.credential_strategy == "config"

    def test_database_parameter_reaches_the_hook(self):
        """Inherited from DuckDBExecuteQueryOperator; asserted here so the subclass cannot regress it."""
        operator = AwsDuckDBOperator(
            task_id="t", sql="SELECT 1", database="/tmp/aws.duckdb", conn_id=AwsDuckDBHook.default_conn_name
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            hook = operator.get_db_hook()
            assert isinstance(hook, AwsDuckDBHook)
            assert hook.get_database() == "/tmp/aws.duckdb"

    def test_runs_sql_without_touching_aws_when_credentials_are_not_needed(self):
        """A query that never reaches S3 still works: the secret is simply not created."""
        operator = AwsDuckDBOperator(
            task_id="t",
            sql="SELECT 21 * 2 AS answer",
            conn_id=AwsDuckDBHook.default_conn_name,
            hook_params={"credential_strategy": "none"},
            do_xcom_push=True,
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert operator.execute({}) == [(42,)]
