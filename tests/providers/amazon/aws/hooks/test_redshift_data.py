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

import logging
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

SQL = "sql"
DATABASE = "database"
STATEMENT_ID = "statement_id"


class TestRedshiftDataHook:
    def test_conn_attribute(self):
        hook = RedshiftDataHook()
        assert hasattr(hook, "conn")
        assert hook.conn.__class__.__name__ == "RedshiftDataAPIService"
        conn = hook.conn
        assert conn is hook.conn  # Cached property
        assert conn is hook.get_conn()  # Same object as returned by `conn` property

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_without_waiting(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        cluster_identifier = "cluster_identifier"

        hook = RedshiftDataHook()
        hook.execute_query(
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            sql=SQL,
            wait_for_completion=False,
        )
        mock_conn.execute_statement.assert_called_once_with(
            Database=DATABASE,
            ClusterIdentifier=cluster_identifier,
            Sql=SQL,
            WithEvent=False,
        )
        mock_conn.describe_statement.assert_not_called()

    @pytest.mark.parametrize(
        "cluster_identifier, workgroup_name",
        [
            (None, None),
            ("some_cluster", "some_workgroup"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_requires_either_cluster_identifier_or_workgroup_name(
        self, mock_conn, cluster_identifier, workgroup_name
    ):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        cluster_identifier = "cluster_identifier"
        workgroup_name = "workgroup_name"

        with pytest.raises(ValueError):
            hook = RedshiftDataHook()
            hook.execute_query(
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                workgroup_name=workgroup_name,
                sql=SQL,
                wait_for_completion=False,
            )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_with_all_parameters_cluster_identifier(self, mock_conn):
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}

        hook = RedshiftDataHook()
        hook.execute_query(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
        )

        mock_conn.execute_statement.assert_called_once_with(
            Database=DATABASE,
            Sql=SQL,
            ClusterIdentifier=cluster_identifier,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            Parameters=parameters,
            WithEvent=False,
        )
        mock_conn.describe_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_with_all_parameters_workgroup_name(self, mock_conn):
        workgroup_name = "workgroup_name"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}

        hook = RedshiftDataHook()
        hook.execute_query(
            sql=SQL,
            database=DATABASE,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
        )

        mock_conn.execute_statement.assert_called_once_with(
            Database=DATABASE,
            Sql=SQL,
            WorkgroupName=workgroup_name,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            Parameters=parameters,
            WithEvent=False,
        )
        mock_conn.describe_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_batch_execute(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        hook = RedshiftDataHook()
        hook.execute_query(
            cluster_identifier=cluster_identifier,
            database=DATABASE,
            db_user=db_user,
            sql=[SQL],
            statement_name=statement_name,
            secret_arn=secret_arn,
        )

        mock_conn.batch_execute_statement.assert_called_once_with(
            Database=DATABASE,
            Sqls=[SQL],
            ClusterIdentifier=cluster_identifier,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            WithEvent=False,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_get_table_primary_key_no_token(self, mock_conn):
        table = "table"
        schema = "schema"
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_conn.get_statement_result.return_value = {
            "Records": [[{"stringValue": "string"}]],
        }

        hook = RedshiftDataHook()

        hook.get_table_primary_key(
            table=table,
            database=DATABASE,
            schema=schema,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
        )

        mock_conn.get_statement_result.assert_called_once_with(Id=STATEMENT_ID)

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_get_table_primary_key_with_token(self, mock_conn):
        table = "table"
        schema = "schema"
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_conn.get_statement_result.side_effect = [
            {
                "NextToken": "token1",
                "Records": [[{"stringValue": "string1"}]],
            },
            {
                "NextToken": "token2",
                "Records": [[{"stringValue": "string2"}]],
            },
            {
                "Records": [[{"stringValue": "string3"}]],
            },
        ]

        hook = RedshiftDataHook()

        hook.get_table_primary_key(
            table=table,
            database=DATABASE,
            schema=schema,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
        )

        assert mock_conn.get_statement_result.call_args_list == [
            (dict(Id=STATEMENT_ID),),
            (dict(Id=STATEMENT_ID, NextToken="token1"),),
            (dict(Id=STATEMENT_ID, NextToken="token2"),),
        ]

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_result_num_rows(self, mock_conn, caplog):
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED", "ResultRows": 123}

        hook = RedshiftDataHook()
        # https://docs.pytest.org/en/stable/how-to/logging.html
        with caplog.at_level(logging.INFO):
            hook.execute_query(
                sql=SQL,
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
                parameters=parameters,
                wait_for_completion=True,
            )
            assert "Processed 123 rows" in caplog.text

        # ensure message is not there when `ResultRows` is not returned
        caplog.clear()
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        with caplog.at_level(logging.INFO):
            hook.execute_query(
                sql=SQL,
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
                parameters=parameters,
                wait_for_completion=True,
            )
            assert "Processed " not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "describe_statement_response, expected_result",
        [
            ({"Status": "PICKED"}, True),
            ({"Status": "STARTED"}, True),
            ({"Status": "SUBMITTED"}, True),
            ({"Status": "FINISHED"}, False),
            ({"Status": "FAILED"}, False),
            ({"Status": "ABORTED"}, False),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_is_still_running(self, mock_client, describe_statement_response, expected_result):
        hook = RedshiftDataHook()
        mock_client.return_value.describe_statement.return_value = describe_statement_response
        response = await hook.is_still_running("uuid")
        assert response == expected_result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "describe_statement_response, expected_result",
        [
            ({"Status": "FINISHED"}, {"status": "success", "statement_id": "uuid"}),
            (
                {"Status": "FAILED", "QueryString": "select 1", "Error": "Test error"},
                {
                    "status": "error",
                    "message": "Error: select 1 query Failed due to, Test error",
                    "statement_id": "uuid",
                    "type": "FAILED",
                },
            ),
            (
                {"Status": "ABORTED"},
                {
                    "status": "error",
                    "message": "The query run was stopped by the user.",
                    "statement_id": "uuid",
                    "type": "ABORTED",
                },
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running")
    async def test_get_query_status(
        self, mock_is_still_running, mock_conn, describe_statement_response, expected_result
    ):
        hook = RedshiftDataHook()
        mock_is_still_running.return_value = False
        mock_conn.return_value.describe_statement.return_value = describe_statement_response
        response = await hook.check_query_is_finished_async(statement_id="uuid")
        assert response == expected_result
