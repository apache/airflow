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
from uuid import uuid4

import pytest

from airflow.providers.amazon.aws.hooks.redshift_data import (
    RedshiftDataHook,
    RedshiftDataQueryAbortedError,
    RedshiftDataQueryFailedError,
)

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
        "cluster_identifier, workgroup_name, session_id",
        [
            (None, None, None),
            ("some_cluster", "some_workgroup", None),
            (None, "some_workgroup", None),
            ("some_cluster", None, None),
            (None, None, "some_session_id"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_requires_one_of_cluster_identifier_or_workgroup_name_or_session_id(
        self, mock_conn, cluster_identifier, workgroup_name, session_id
    ):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        cluster_identifier = "cluster_identifier"
        workgroup_name = "workgroup_name"
        hook = RedshiftDataHook()
        with pytest.raises(ValueError):
            hook.execute_query(
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                workgroup_name=workgroup_name,
                sql=SQL,
                wait_for_completion=False,
                session_id=session_id,
            )

    @pytest.mark.parametrize(
        "cluster_identifier, workgroup_name, session_id",
        [
            (None, None, None),
            ("some_cluster", "some_workgroup", None),
            (None, "some_workgroup", None),
            ("some_cluster", None, None),
            (None, None, "some_session_id"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_session_keep_alive_seconds_valid(
        self, mock_conn, cluster_identifier, workgroup_name, session_id
    ):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        cluster_identifier = "cluster_identifier"
        workgroup_name = "workgroup_name"
        hook = RedshiftDataHook()
        with pytest.raises(ValueError):
            hook.execute_query(
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                workgroup_name=workgroup_name,
                sql=SQL,
                wait_for_completion=False,
                session_id=session_id,
            )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_session_id_valid(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        cluster_identifier = "cluster_identifier"
        workgroup_name = "workgroup_name"
        hook = RedshiftDataHook()
        with pytest.raises(ValueError):
            hook.execute_query(
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                workgroup_name=workgroup_name,
                sql=SQL,
                wait_for_completion=False,
                session_id="not_a_uuid",
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
    def test_execute_with_new_session(self, mock_conn):
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        mock_conn.execute_statement.return_value = {
            "Id": STATEMENT_ID,
            "SessionId": "session_id",
        }
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}

        hook = RedshiftDataHook()
        output = hook.execute_query(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            session_keep_alive_seconds=123,
        )
        assert output.statement_id == STATEMENT_ID
        assert output.session_id == "session_id"

        mock_conn.execute_statement.assert_called_once_with(
            Database=DATABASE,
            Sql=SQL,
            ClusterIdentifier=cluster_identifier,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            Parameters=parameters,
            WithEvent=False,
            SessionKeepAliveSeconds=123,
        )
        mock_conn.describe_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_reuse_session(self, mock_conn):
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        mock_conn.execute_statement.return_value = {
            "Id": STATEMENT_ID,
            "SessionId": "session_id",
        }
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        hook = RedshiftDataHook()
        session_id = str(uuid4())
        output = hook.execute_query(
            database=None,
            sql=SQL,
            statement_name=statement_name,
            parameters=parameters,
            session_id=session_id,
        )
        assert output.statement_id == STATEMENT_ID
        assert output.session_id == "session_id"

        mock_conn.execute_statement.assert_called_once_with(
            Sql=SQL,
            StatementName=statement_name,
            Parameters=parameters,
            WithEvent=False,
            SessionId=session_id,
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
        mock_conn.describe_statement.return_value = {
            "Status": "FINISHED",
            "ResultRows": 123,
        }

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
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.async_conn"
    )
    async def test_is_still_running(
        self, mock_conn, describe_statement_response, expected_result
    ):
        hook = RedshiftDataHook()
        mock_conn.__aenter__.return_value.describe_statement.return_value = (
            describe_statement_response
        )
        response = await hook.is_still_running("uuid")
        assert response == expected_result

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.async_conn"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running"
    )
    async def test_check_query_is_finished_async(self, mock_is_still_running, mock_conn):
        hook = RedshiftDataHook()
        mock_is_still_running.return_value = False
        mock_conn.describe_statement = mock.AsyncMock()
        mock_conn.__aenter__.return_value.describe_statement.return_value = {
            "Id": "uuid",
            "Status": "FINISHED",
        }
        is_finished = await hook.check_query_is_finished_async(statement_id="uuid")
        assert is_finished is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "describe_statement_response, expected_exception",
        (
            (
                {
                    "Id": "uuid",
                    "Status": "FAILED",
                    "QueryString": "select 1",
                    "Error": "Test error",
                },
                RedshiftDataQueryFailedError,
            ),
            ({"Id": "uuid", "Status": "ABORTED"}, RedshiftDataQueryAbortedError),
        ),
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.async_conn"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running"
    )
    async def test_check_query_is_finished_async_exception(
        self,
        mock_is_still_running,
        mock_conn,
        describe_statement_response,
        expected_exception,
    ):
        hook = RedshiftDataHook()
        mock_is_still_running.return_value = False
        mock_conn.__aenter__.return_value.describe_statement.return_value = (
            describe_statement_response
        )
        with pytest.raises(expected_exception):
            await hook.check_query_is_finished_async(statement_id="uuid")
