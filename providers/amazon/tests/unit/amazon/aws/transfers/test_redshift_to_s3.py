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

from copy import deepcopy
from unittest import mock

import pytest
from boto3.session import Session

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)

from tests_common.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestRedshiftToS3Transfer:
    @pytest.mark.parametrize(
        ("table_as_file_name", "expected_s3_key"), [[True, "key/table_"], [False, "key"]]
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_table_unloading(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table_as_file_name,
        expected_s3_key,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    @pytest.mark.parametrize(
        ("table_as_file_name", "expected_s3_key"), [[True, "key/table_"], [False, "key"]]
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_execute_sts_token(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table_as_file_name,
        expected_s3_key,
    ):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "token"
        mock_session.return_value = Session(access_key, secret_key, token)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = token
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert token in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    @pytest.mark.parametrize(
        ("table", "table_as_file_name", "expected_s3_key"),
        [
            ["table", True, "key/table_"],
            ["table", False, "key"],
            [None, False, "key"],
            [None, True, "key"],
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_custom_select_query_unloading(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table,
        table_as_file_name,
        expected_s3_key,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]
        select_query = "select column from table"

        op = RedshiftToS3Operator(
            select_query=select_query,
            table=table,
            table_as_file_name=table_as_file_name,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    @pytest.mark.parametrize(
        ("table_as_file_name", "expected_s3_key", "select_query", "expected_query"),
        [
            [
                True,
                "key/table_",
                "SELECT 'Single Quotes Break this Operator'",
                "SELECT 'Single Quotes Break this Operator'",
            ],
            [
                False,
                "key",
                "SELECT 'Single Quotes Break this Operator'",
                "SELECT 'Single Quotes Break this Operator'",
            ],
            [
                True,
                "key/table_",
                "SELECT ''Single Quotes Break this Operator''",
                "SELECT 'Single Quotes Break this Operator'",
            ],
            [
                False,
                "key",
                "SELECT ''Single Quotes Break this Operator''",
                "SELECT 'Single Quotes Break this Operator'",
            ],
            [False, "key", "SELECT ''", "SELECT ''"],
            [
                False,
                "key",
                "SELECT ''Single Quotes '' || ''Break this Operator''",
                "SELECT 'Single Quotes ' || 'Break this Operator'",
            ],
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_custom_select_query_unloading_with_single_quotes(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table_as_file_name,
        expected_s3_key,
        select_query,
        expected_query,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = ["HEADER"]

        op = RedshiftToS3Operator(
            select_query=select_query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(credentials_block, select_query, s3_key, unload_options)

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)
        assert f"UNLOAD ($${expected_query}$$)" in unload_query

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_custom_select_query_has_precedence_over_table_and_schema(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]
        select_query = "select column from table"

        op = RedshiftToS3Operator(
            select_query=select_query,
            table="table",
            schema="schema",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(credentials_block, select_query, "key/table_", unload_options)

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_default_select_query_used_when_table_and_schema_missing(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]
        default_query = "SELECT * FROM schema.table"

        op = RedshiftToS3Operator(
            table="table",
            schema="schema",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(credentials_block, default_query, "key/table_", unload_options)

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    def test_lack_of_select_query_and_schema_and_table_raises_error(self):
        op = RedshiftToS3Operator(
            s3_bucket="bucket",
            s3_key="key",
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        with pytest.raises(
            ValueError, match="Please specify either a table or `select_query` to fetch the data."
        ):
            op.execute(None)

    @pytest.mark.parametrize(
        ("table_as_file_name", "expected_s3_key"), [[True, "key/table_"], [False, "key"]]
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_table_unloading_role_arn(
        self,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table_as_file_name,
        expected_s3_key,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        extra = {"role_arn": "arn:aws:iam::112233445566:role/myRole"}
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection(extra=extra)
        mock_hook.return_value = Connection(extra=extra)
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = f"aws_iam_role={extra['role_arn']}"

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert extra["role_arn"] in unload_query
        assert_equal_ignore_multiple_spaces(mock_run.call_args.args[0], unload_query)

    @pytest.mark.parametrize("param", ["sql", "parameters"])
    def test_invalid_param_in_redshift_data_api_kwargs(self, param):
        """
        Test passing invalid param in RS Data API kwargs raises an error
        """
        redshift_operator = RedshiftToS3Operator(
            s3_bucket="s3_bucket",
            s3_key="s3_key",
            select_query="select_query",
            task_id="task_id",
            dag=None,
            redshift_data_api_kwargs={param: "param"},
        )
        with pytest.raises(AirflowException):
            redshift_operator.execute(None)

    @pytest.mark.parametrize(
        ("table_as_file_name", "expected_s3_key"), [[True, "key/table_"], [False, "key"]]
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_table_unloading_using_redshift_data_api(
        self,
        mock_rs,
        mock_run,
        mock_session,
        mock_connection,
        mock_hook,
        table_as_file_name,
        expected_s3_key,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()
        mock_rs.execute_statement.return_value = {"Id": "STATEMENT_ID"}
        mock_rs.describe_statement.return_value = {"Status": "FINISHED"}

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            "HEADER",
        ]
        # RS Data API params
        database = "database"
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
            redshift_data_api_kwargs=dict(
                database=database,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
            ),
        )

        op.execute(None)

        unload_options = "\n\t\t\t".join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        mock_run.assert_not_called()
        assert access_key in unload_query
        assert secret_key in unload_query

        mock_rs.execute_statement.assert_called_once()
        # test with all args besides sql
        _call = deepcopy(mock_rs.execute_statement.call_args.kwargs)
        _call.pop("Sql")
        assert _call == dict(
            Database=database,
            ClusterIdentifier=cluster_identifier,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            WithEvent=False,
        )
        mock_rs.describe_statement.assert_called_once_with(
            Id="STATEMENT_ID",
        )
        # test sql arg
        assert_equal_ignore_multiple_spaces(mock_rs.execute_statement.call_args.kwargs["Sql"], unload_query)

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_default(
        self, mock_get_facets, mock_run, mock_session, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                conn_id="aws_conn_id",
                conn_type="aws",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="redshift_conn_id",
                conn_type="redshift",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None

        mock_facets = {
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
            "documentation": DocumentationDatasetFacet(description="mock_description"),
        }
        mock_get_facets.return_value = mock_facets

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
        )
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)
        # Hook called only one time - on operator execution - we mocked querying to fetch schema
        assert mock_run.call_count == 1

        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].name == f"{s3_key}/{table}_"
        assert lineage.outputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.inputs[0].name == f"database.{schema}.{table}"
        assert lineage.inputs[0].namespace == "redshift://cluster.region:5439"

        assert lineage.inputs[0].facets == mock_facets
        assert lineage.outputs[0].facets == {
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "col": Fields(
                        inputFields=[
                            InputField(
                                namespace="redshift://cluster.region:5439",
                                name=f"database.{schema}.{table}",
                                field="col",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    )
                }
            ),
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
        }

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_with_select_query(
        self, mock_get_facets, mock_run, mock_session, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                conn_id="redshift_conn_id",
                conn_type="redshift",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="aws_conn_id",
                conn_type="aws",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_facets = {
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
            "documentation": DocumentationDatasetFacet(description="mock_description"),
        }
        mock_get_facets.return_value = mock_facets

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        query = """
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            o.order_id,
            o.order_date,
            o.total_amount
        FROM
            schema1.customers c
        INNER JOIN
            schema2.orders o
        ON
            c.customer_id = o.customer_id
        ORDER BY
            o.order_date DESC;
        """
        op = RedshiftToS3Operator(
            schema=schema,  # should be ignored
            table=table,  # should be ignored
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            table_as_file_name=False,
            select_query=query,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
        )
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)

        assert len(lineage.inputs) == 2
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].name == s3_key
        assert lineage.outputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.inputs[0].name == "database.schema1.customers"
        assert lineage.inputs[0].namespace == "redshift://cluster.region:5439"
        assert lineage.inputs[1].name == "database.schema2.orders"
        assert lineage.inputs[1].namespace == "redshift://cluster.region:5439"

        assert lineage.inputs[0].facets == mock_facets
        assert lineage.outputs[0].facets == {}

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.region_name",
        new_callable=mock.PropertyMock,
    )
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_using_redshift_data_api(
        self, mock_get_facets, mock_rs_region, mock_rs, mock_session, mock_connection, mock_hook
    ):
        """
        Using the Redshift Data API instead of the SQL-based connection
        """
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None

        mock_hook.return_value = Connection()
        mock_rs.execute_statement.return_value = {"Id": "STATEMENT_ID"}
        mock_rs.describe_statement.return_value = {"Status": "FINISHED"}

        mock_rs_region.return_value = "region"
        mock_facets = {
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
            "documentation": DocumentationDatasetFacet(description="mock_description"),
        }
        mock_get_facets.return_value = mock_facets

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"

        # RS Data API params
        database = "database"
        cluster_identifier = "cluster"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            redshift_data_api_kwargs=dict(
                database=database,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
            ),
        )
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)

        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].name == f"{s3_key}/{table}_"
        assert lineage.outputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.inputs[0].name == f"database.{schema}.{table}"
        assert lineage.inputs[0].namespace == "redshift://cluster.region:5439"

        assert lineage.inputs[0].facets == mock_facets
        assert lineage.outputs[0].facets == {
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "col": Fields(
                        inputFields=[
                            InputField(
                                namespace="redshift://cluster.region:5439",
                                name=f"database.{schema}.{table}",
                                field="col",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    )
                }
            ),
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
        }

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.region_name",
        new_callable=mock.PropertyMock,
    )
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_data_and_sql_hooks_aligned(
        self, mock_get_facets, mock_rs_region, mock_rs, mock_run, mock_session, create_connection_without_db
    ):
        """
        Ensuring both supported hooks - RedshiftDataHook and RedshiftSQLHook return same lineage.
        """
        create_connection_without_db(
            Connection(
                conn_id="redshift_conn_id",
                conn_type="redshift",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="aws_conn_id",
                conn_type="aws",
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
            )
        )

        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        mock_rs.execute_statement.return_value = {"Id": "STATEMENT_ID"}
        mock_rs.describe_statement.return_value = {"Status": "FINISHED"}

        mock_rs_region.return_value = "region"
        mock_facets = {
            "schema": SchemaDatasetFacet(fields=[SchemaDatasetFacetFields(name="col", type="STRING")]),
            "documentation": DocumentationDatasetFacet(description="mock_description"),
        }
        mock_get_facets.return_value = mock_facets

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"

        # RS Data API params
        database = "database"
        cluster_identifier = "cluster"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op_rs_data = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            redshift_data_api_kwargs=dict(
                database=database,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
            ),
        )
        op_rs_data.execute(None)
        rs_data_lineage = op_rs_data.get_openlineage_facets_on_complete(None)

        op_rs_sql = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
        )
        op_rs_sql.execute(None)
        rs_sql_lineage = op_rs_sql.get_openlineage_facets_on_complete(None)

        assert len(rs_sql_lineage.inputs) == 1
        assert len(rs_sql_lineage.outputs) == 1
        assert rs_sql_lineage.inputs == rs_data_lineage.inputs
        assert rs_sql_lineage.outputs == rs_data_lineage.outputs
        assert rs_sql_lineage.job_facets == rs_data_lineage.job_facets
        assert rs_sql_lineage.run_facets == rs_data_lineage.run_facets
