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

from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook as OriginalRedshiftSQLHook
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SQLJobFacet,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

MOCK_REGION_NAME = "eu-north-1"


class TestRedshiftSQLOpenLineage:
    @patch.dict("os.environ", AIRFLOW_CONN_AWS_DEFAULT=f"aws://?region_name={MOCK_REGION_NAME}")
    @pytest.mark.parametrize(
        "connection_host, connection_extra, expected_identity, is_over_210, expected_schemaname",
        [
            # test without a connection host but with a cluster_identifier in connection extra
            (
                None,
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                f"cluster_identifier_from_extra.{MOCK_REGION_NAME}",
                True,
                "database.public",
            ),
            # test with a connection host and without a cluster_identifier in connection extra
            (
                "cluster_identifier_from_host.id.my_region.redshift.amazonaws.com",
                {"iam": True},
                "cluster_identifier_from_host.my_region",
                True,
                "database.public",
            ),
            # test with both connection host and cluster_identifier in connection extra
            (
                "cluster_identifier_from_host.x.y",
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                f"cluster_identifier_from_extra.{MOCK_REGION_NAME}",
                True,
                "database.public",
            ),
            # test when hostname doesn't match pattern
            ("1.2.3.4", {}, "1.2.3.4", True, "database.public"),
            # test with Airflow below 2.10 not using Hook connection
            (
                "cluster_identifier_from_host.id.my_region.redshift.amazonaws.com",
                {"iam": True},
                "cluster_identifier_from_host.my_region",
                False,
                "public",
            ),
        ],
    )
    @patch(
        "airflow.providers.amazon.aws.hooks.redshift_sql._IS_AIRFLOW_2_10_OR_HIGHER",
        new_callable=PropertyMock,
    )
    @patch("airflow.providers.openlineage.utils.utils.IS_AIRFLOW_2_10_OR_HIGHER", new_callable=PropertyMock)
    @patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    def test_execute_openlineage_events(
        self,
        mock_aws_hook_conn,
        mock_ol_utils,
        mock_redshift_sql,
        connection_host,
        connection_extra,
        expected_identity,
        is_over_210,
        expected_schemaname,
        # self, mock_aws_hook_conn, connection_host, connection_extra, expected_identity, is_below_2_10, expected_schemaname
    ):
        mock_ol_utils.__bool__ = lambda x: is_over_210
        mock_redshift_sql.__bool__ = lambda x: is_over_210
        DB_NAME = "database"
        DB_SCHEMA_NAME = "public"

        ANOTHER_DB_NAME = "another_db"
        ANOTHER_DB_SCHEMA = "another_schema"

        # Mock AWS Connection
        mock_aws_hook_conn.get_cluster_credentials.return_value = {
            "DbPassword": "aws_token",
            "DbUser": "IAM:user",
        }

        class RedshiftSQLHook(OriginalRedshiftSQLHook):
            get_conn = MagicMock(name="conn")
            get_connection = MagicMock()

            def get_first(self, *_):
                self.log.error("CALLING FIRST")
                return [f"{DB_NAME}.{DB_SCHEMA_NAME}"]

        dbapi_hook = RedshiftSQLHook()

        class RedshiftOperatorForTest(SQLExecuteQueryOperator):
            def get_db_hook(self):
                return dbapi_hook

        sql = (
            "INSERT INTO Test_table\n"
            "SELECT t1.*, t2.additional_constant FROM ANOTHER_db.another_schema.popular_orders_day_of_week t1\n"
            "JOIN little_table t2 ON t1.order_day_of_week = t2.order_day_of_week;\n"
            "FORGOT TO COMMENT"
        )
        op = RedshiftOperatorForTest(task_id="redshift-operator", sql=sql)
        rows = [
            [
                (
                    ANOTHER_DB_SCHEMA,
                    "popular_orders_day_of_week",
                    "order_day_of_week",
                    1,
                    "varchar",
                    ANOTHER_DB_NAME,
                ),
                (
                    ANOTHER_DB_SCHEMA,
                    "popular_orders_day_of_week",
                    "order_placed_on",
                    2,
                    "timestamp",
                    ANOTHER_DB_NAME,
                ),
                (
                    ANOTHER_DB_SCHEMA,
                    "popular_orders_day_of_week",
                    "orders_placed",
                    3,
                    "int4",
                    ANOTHER_DB_NAME,
                ),
                (DB_SCHEMA_NAME, "little_table", "order_day_of_week", 1, "varchar", DB_NAME),
                (DB_SCHEMA_NAME, "little_table", "additional_constant", 2, "varchar", DB_NAME),
            ],
            [
                (DB_SCHEMA_NAME, "test_table", "order_day_of_week", 1, "varchar", DB_NAME),
                (DB_SCHEMA_NAME, "test_table", "order_placed_on", 2, "timestamp", DB_NAME),
                (DB_SCHEMA_NAME, "test_table", "orders_placed", 3, "int4", DB_NAME),
                (DB_SCHEMA_NAME, "test_table", "additional_constant", 4, "varchar", DB_NAME),
            ],
        ]
        dbapi_hook.get_connection.return_value = Connection(
            conn_id="redshift_default",
            conn_type="redshift",
            host=connection_host,
            extra=connection_extra,
        )
        dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = rows

        lineage = op.get_openlineage_facets_on_start()
        if is_over_210:
            assert dbapi_hook.get_conn.return_value.cursor.return_value.execute.mock_calls == [
                call(
                    "SELECT SVV_REDSHIFT_COLUMNS.schema_name, "
                    "SVV_REDSHIFT_COLUMNS.table_name, "
                    "SVV_REDSHIFT_COLUMNS.column_name, "
                    "SVV_REDSHIFT_COLUMNS.ordinal_position, "
                    "SVV_REDSHIFT_COLUMNS.data_type, "
                    "SVV_REDSHIFT_COLUMNS.database_name \n"
                    "FROM SVV_REDSHIFT_COLUMNS \n"
                    f"WHERE SVV_REDSHIFT_COLUMNS.schema_name = '{expected_schemaname}' "
                    "AND SVV_REDSHIFT_COLUMNS.table_name IN ('little_table') "
                    "OR SVV_REDSHIFT_COLUMNS.database_name = 'another_db' "
                    "AND SVV_REDSHIFT_COLUMNS.schema_name = 'another_schema' AND "
                    "SVV_REDSHIFT_COLUMNS.table_name IN ('popular_orders_day_of_week')"
                ),
                call(
                    "SELECT SVV_REDSHIFT_COLUMNS.schema_name, "
                    "SVV_REDSHIFT_COLUMNS.table_name, "
                    "SVV_REDSHIFT_COLUMNS.column_name, "
                    "SVV_REDSHIFT_COLUMNS.ordinal_position, "
                    "SVV_REDSHIFT_COLUMNS.data_type, "
                    "SVV_REDSHIFT_COLUMNS.database_name \n"
                    "FROM SVV_REDSHIFT_COLUMNS \n"
                    f"WHERE SVV_REDSHIFT_COLUMNS.schema_name = '{expected_schemaname}' "
                    "AND SVV_REDSHIFT_COLUMNS.table_name IN ('Test_table')"
                ),
            ]
        else:
            assert dbapi_hook.get_conn.return_value.cursor.return_value.execute.mock_calls == []
        expected_namespace = f"redshift://{expected_identity}:5439"

        if is_over_210:
            assert lineage.inputs == [
                Dataset(
                    namespace=expected_namespace,
                    name=f"{ANOTHER_DB_NAME}.{ANOTHER_DB_SCHEMA}.popular_orders_day_of_week",
                    facets={
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
                                SchemaDatasetFacetFields(name="order_placed_on", type="timestamp"),
                                SchemaDatasetFacetFields(name="orders_placed", type="int4"),
                            ]
                        )
                    },
                ),
                Dataset(
                    namespace=expected_namespace,
                    name=f"{DB_NAME}.{DB_SCHEMA_NAME}.little_table",
                    facets={
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
                                SchemaDatasetFacetFields(name="additional_constant", type="varchar"),
                            ]
                        )
                    },
                ),
            ]
            assert lineage.outputs == [
                Dataset(
                    namespace=expected_namespace,
                    name=f"{DB_NAME}.{DB_SCHEMA_NAME}.test_table",
                    facets={
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
                                SchemaDatasetFacetFields(name="order_placed_on", type="timestamp"),
                                SchemaDatasetFacetFields(name="orders_placed", type="int4"),
                                SchemaDatasetFacetFields(name="additional_constant", type="varchar"),
                            ]
                        ),
                        "columnLineage": ColumnLineageDatasetFacet(
                            fields={
                                "additional_constant": Fields(
                                    inputFields=[
                                        InputField(
                                            namespace=expected_namespace,
                                            name="database.public.little_table",
                                            field="additional_constant",
                                        )
                                    ],
                                    transformationDescription="",
                                    transformationType="",
                                )
                            }
                        ),
                    },
                )
            ]

        assert lineage.job_facets == {"sql": SQLJobFacet(query=sql)}

        assert lineage.run_facets["extractionError"].failedTasks == 1
