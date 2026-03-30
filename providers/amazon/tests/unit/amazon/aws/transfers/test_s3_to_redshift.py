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

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.compat.openlineage.facet import (
    DocumentationDatasetFacet,
    LifecycleStateChange,
    LifecycleStateChangeDatasetFacet,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestS3ToRedshiftTransfer:
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_execute(self, mock_run, mock_session, mock_connection, mock_hook):
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        expected_copy_query = """
                        COPY schema.table
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        ;
                     """
        actual_copy_query = mock_run.call_args.args[0]

        assert mock_run.call_count == 1
        assert access_key in actual_copy_query
        assert secret_key in actual_copy_query
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_execute_with_column_list(self, mock_run, mock_session, mock_connection, mock_hook):
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
        column_list = ["column_1", "column_2"]
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            column_list=column_list,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        expected_copy_query = """
                        COPY schema.table (column_1, column_2)
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        ;
                     """
        actual_copy_query = mock_run.call_args.args[0]

        assert mock_run.call_count == 1
        assert access_key in actual_copy_query
        assert secret_key in actual_copy_query
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_replace(self, mock_run, mock_session, mock_connection, mock_hook):
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            method="REPLACE",
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        copy_statement = """
                        COPY schema.table
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        ;
                     """
        delete_statement = f"DELETE FROM {schema}.{table};"
        transaction = f"""
                    BEGIN;
                    {delete_statement}
                    {copy_statement}
                    COMMIT
                    """
        assert_equal_ignore_multiple_spaces("\n".join(mock_run.call_args.args[0]), transaction)

        assert mock_run.call_count == 1

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_upsert(self, mock_run, mock_session, mock_connection, mock_hook):
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            method="UPSERT",
            upsert_keys=["id"],
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)

        copy_statement = f"""
                        COPY #{table}
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        ;
                     """
        transaction = f"""
                    CREATE TABLE #{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);
                    {copy_statement}
                    BEGIN;
                    DELETE FROM {schema}.{table} USING #{table} WHERE {table}.id = #{table}.id;
                    INSERT INTO {schema}.{table} SELECT * FROM #{table};
                    COMMIT
                    """
        assert_equal_ignore_multiple_spaces("\n".join(mock_run.call_args.args[0]), transaction)

        assert mock_run.call_count == 1

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_execute_sts_token(self, mock_run, mock_session, mock_connection, mock_hook):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "aws_secret_token"
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        expected_copy_query = """
                            COPY schema.table
                            FROM 's3://bucket/key'
                            credentials
                            'aws_access_key_id=ASIA_aws_access_key_id;aws_secret_access_key=aws_secret_access_key;token=aws_secret_token'
                            ;
                         """
        actual_copy_query = mock_run.call_args.args[0]

        assert access_key in actual_copy_query
        assert secret_key in actual_copy_query
        assert token in actual_copy_query
        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_execute_role_arn(self, mock_run, mock_session, mock_connection, mock_hook):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "aws_secret_token"
        extra = {"role_arn": "arn:aws:iam::112233445566:role/myRole"}

        mock_session.return_value = Session(access_key, secret_key, token)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = token

        mock_connection.return_value = Connection(extra=extra)
        mock_hook.return_value = Connection(extra=extra)

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        expected_copy_query = """
                            COPY schema.table
                            FROM 's3://bucket/key'
                            credentials
                            'aws_iam_role=arn:aws:iam::112233445566:role/myRole'
                            ;
                         """
        actual_copy_query = mock_run.call_args.args[0]

        assert extra["role_arn"] in actual_copy_query
        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    def test_different_region(self, mock_run, mock_session, mock_connection, mock_hook):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        extra = {"region": "eu-central-1"}
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)
        expected_copy_query = """
                        COPY schema.table
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        region 'eu-central-1'
                        ;
                     """
        actual_copy_query = mock_run.call_args.args[0]

        assert access_key in actual_copy_query
        assert secret_key in actual_copy_query
        assert extra["region"] in actual_copy_query
        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    def test_execute_unavailable_method(self):
        """
        Test execute unavailable method
        """
        with pytest.raises(AirflowException):
            S3ToRedshiftOperator(
                schema="schema",
                table="table",
                s3_bucket="bucket",
                s3_key="key",
                method="unavailable_method",
                task_id="task_id",
                dag=None,
            ).execute({})

    @pytest.mark.parametrize("param", ["sql", "parameters"])
    def test_invalid_param_in_redshift_data_api_kwargs(self, param):
        """
        Test passing invalid param in RS Data API kwargs raises an error
        """
        with pytest.raises(AirflowException):
            S3ToRedshiftOperator(
                schema="schema",
                table="table",
                s3_bucket="bucket",
                s3_key="key",
                task_id="task_id",
                dag=None,
                redshift_data_api_kwargs={param: "param"},
            )

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_using_redshift_data_api(self, mock_rs, mock_run, mock_session, mock_connection, mock_hook):
        """
        Using the Redshift Data API instead of the SQL-based connection
        """
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
        copy_options = ""

        # RS Data API params
        database = "database"
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
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

        mock_run.assert_not_called()

        mock_rs.execute_statement.assert_called_once()
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

        expected_copy_query = """
                        COPY schema.table
                        FROM 's3://bucket/key'
                        credentials
                        'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_secret_access_key'
                        ;
                     """
        actual_copy_query = mock_rs.execute_statement.call_args.kwargs["Sql"]

        mock_rs.describe_statement.assert_called_once_with(
            Id="STATEMENT_ID",
        )

        assert access_key in actual_copy_query
        assert secret_key in actual_copy_query
        assert_equal_ignore_multiple_spaces(actual_copy_query, expected_copy_query)

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_default(
        self, mock_get_facets, mock_run, mock_session, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
                conn_id="redshift_conn_id",
                conn_type="redshift",
            )
        )
        create_connection_without_db(
            Connection(
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                extra={},
                conn_id="aws_conn_id",
                conn_type="aws",
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
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
        assert lineage.inputs[0].name == s3_key
        assert lineage.inputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.outputs[0].name == f"database.{schema}.{table}"
        assert lineage.outputs[0].namespace == "redshift://cluster.region:5439"

        assert lineage.outputs[0].facets == mock_facets
        assert lineage.inputs[0].facets == {}

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.run")
    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_facets_from_redshift_table")
    def test_get_openlineage_facets_on_complete_replace(
        self, mock_get_facets, mock_run, mock_session, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                conn_type="redshift",
                conn_id="redshift_conn_id",
            )
        )
        create_connection_without_db(
            Connection(
                schema="database",
                port=5439,
                host="cluster.id.region.redshift.amazonaws.com",
                conn_type="aws",
                conn_id="aws_conn_id",
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
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            method="REPLACE",
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
        )
        op.execute(None)

        lineage = op.get_openlineage_facets_on_complete(None)

        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0].name == s3_key
        assert lineage.inputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.outputs[0].name == f"database.{schema}.{table}"
        assert lineage.outputs[0].namespace == "redshift://cluster.region:5439"

        assert lineage.outputs[0].facets == {
            **mock_facets,
            "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                lifecycleStateChange=LifecycleStateChange.OVERWRITE
            ),
        }
        assert lineage.inputs[0].facets == {}

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
        copy_options = ""

        # RS Data API params
        database = "database"
        cluster_identifier = "cluster"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            method="REPLACE",
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
        assert lineage.inputs[0].name == s3_key
        assert lineage.inputs[0].namespace == f"s3://{s3_bucket}"
        assert lineage.outputs[0].name == f"database.{schema}.{table}"
        assert lineage.outputs[0].namespace == "redshift://cluster.region:5439"

        assert lineage.outputs[0].facets == {
            **mock_facets,
            "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                lifecycleStateChange=LifecycleStateChange.OVERWRITE
            ),
        }
        assert lineage.inputs[0].facets == {}

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
        copy_options = ""

        # RS Data API params
        database = "database"
        cluster_identifier = "cluster"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        op_rs_data = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
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

        op_rs_sql = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op_rs_sql.execute(None)
        rs_sql_lineage = op_rs_sql.get_openlineage_facets_on_complete(None)

        assert len(rs_sql_lineage.inputs) == 1
        assert len(rs_sql_lineage.outputs) == 1
        assert rs_sql_lineage.inputs == rs_data_lineage.inputs
        assert rs_sql_lineage.outputs == rs_data_lineage.outputs
        assert rs_sql_lineage.job_facets == rs_data_lineage.job_facets
        assert rs_sql_lineage.run_facets == rs_data_lineage.run_facets
