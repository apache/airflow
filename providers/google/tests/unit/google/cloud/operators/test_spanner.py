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
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.operators.spanner import (
    SpannerDeleteDatabaseInstanceOperator,
    SpannerDeleteInstanceOperator,
    SpannerDeployDatabaseInstanceOperator,
    SpannerDeployInstanceOperator,
    SpannerQueryDatabaseInstanceOperator,
    SpannerUpdateDatabaseInstanceOperator,
)
from airflow.providers.openlineage.sqlparser import DatabaseInfo

PROJECT_ID = "project-id"
INSTANCE_ID = "instance-id"
DB_ID = "db1"
CONFIG_NAME = "projects/project-id/instanceConfigs/eur3"
NODE_COUNT = "1"
DISPLAY_NAME = "Test Instance"
INSERT_QUERY = "INSERT my_table1 (id, name) VALUES (1, 'One')"
INSERT_QUERY_2 = "INSERT my_table2 (id, name) VALUES (1, 'One')"
CREATE_QUERY = "CREATE TABLE my_table1 (id INT64, name STRING(100))"
CREATE_QUERY_2 = "CREATE TABLE my_table2 (id INT64, name STRING(100))"
DDL_STATEMENTS = [CREATE_QUERY, CREATE_QUERY_2]
TASK_ID = "task-id"

SCHEMA_ROWS = {
    "public.orders": [
        ("public", "orders", "id", 1, "INT64"),
        ("public", "orders", "amount", 2, "FLOAT64"),
    ],
    "public.staging": [
        ("public", "staging", "id", 1, "INT64"),
        ("public", "staging", "amount", 2, "FLOAT64"),
    ],
    "public.customers": [
        ("public", "customers", "id", 1, "INT64"),
        ("public", "customers", "name", 2, "STRING(100)"),
        ("public", "customers", "customer_id", 3, "INT64"),
    ],
    "public.logs": [
        ("public", "logs", "id", 1, "INT64"),
        ("public", "logs", "message", 2, "STRING(100)"),
    ],
    "public.t1": [("public", "t1", "col1", 1, "STRING(100)")],
    "public.t2": [("public", "t2", "col1", 1, "STRING(100)")],
    "public.t3": [("public", "t3", "id", 1, "INT64")],
    # example of explicit non-default schema
    "myschema.orders": [("myschema", "orders", "id", 1, "INT64")],
}


class TestCloudSpanner:
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_create(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
        )
        mock_hook.return_value.update_instance.assert_not_called()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_create_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeployInstanceOperator(
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
        )
        mock_hook.return_value.update_instance.assert_not_called()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_update(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
        )
        mock_hook.return_value.create_instance.assert_not_called()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_update_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
        )
        mock_hook.return_value.create_instance.assert_not_called()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_create_aborts_and_succeeds_if_instance_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_not_called()
        assert result is None

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "exp_msg"),
        [
            ("", INSTANCE_ID, "project_id"),
            (PROJECT_ID, "", "instance_id"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_create_ex_if_param_missing(self, mock_hook, project_id, instance_id, exp_msg):
        with pytest.raises(AirflowException) as ctx:
            SpannerDeployInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                configuration_name=CONFIG_NAME,
                node_count=int(NODE_COUNT),
                display_name=DISPLAY_NAME,
                task_id="id",
            )
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_delete(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeleteInstanceOperator(project_id=PROJECT_ID, instance_id=INSTANCE_ID, task_id="id")
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_delete_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeleteInstanceOperator(instance_id=INSTANCE_ID, task_id="id")
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_delete_aborts_and_succeeds_if_instance_does_not_exist(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeleteInstanceOperator(project_id=PROJECT_ID, instance_id=INSTANCE_ID, task_id="id")
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_not_called()
        assert result

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "exp_msg"),
        [
            ("", INSTANCE_ID, "project_id"),
            (PROJECT_ID, "", "instance_id"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_delete_ex_if_param_missing(self, mock_hook, project_id, instance_id, exp_msg):
        with pytest.raises(AirflowException) as ctx:
            SpannerDeleteInstanceOperator(project_id=project_id, instance_id=instance_id, task_id="id")
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_query(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = [3]
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=INSERT_QUERY,
            task_id="id",
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, queries=[INSERT_QUERY]
        )
        assert result == [3]

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_query_missing_project_id(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = [3]
        op = SpannerQueryDatabaseInstanceOperator(
            instance_id=INSTANCE_ID, database_id=DB_ID, query=INSERT_QUERY, task_id="id"
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID, queries=[INSERT_QUERY]
        )
        assert result == [3]

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "database_id", "query", "exp_msg"),
        [
            ("", INSTANCE_ID, DB_ID, INSERT_QUERY, "project_id"),
            (PROJECT_ID, "", DB_ID, INSERT_QUERY, "instance_id"),
            (PROJECT_ID, INSTANCE_ID, "", INSERT_QUERY, "database_id"),
            (PROJECT_ID, INSTANCE_ID, DB_ID, "", "query"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_query_ex_if_param_missing(
        self, mock_hook, project_id, instance_id, database_id, query, exp_msg
    ):
        with pytest.raises(AirflowException) as ctx:
            SpannerQueryDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                query=query,
                task_id="id",
            )
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_query_dml(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=INSERT_QUERY,
            task_id="id",
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, queries=[INSERT_QUERY]
        )

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_instance_query_dml_list(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=[INSERT_QUERY, INSERT_QUERY_2],
            task_id="id",
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            queries=[INSERT_QUERY, INSERT_QUERY_2],
        )

    @pytest.mark.parametrize(
        ("sql", "expected_inputs", "expected_outputs", "expected_lineage"),
        [
            ("SELECT id, amount FROM public.orders", ["db1.public.orders"], [], {}),
            (
                "INSERT INTO public.orders (id, amount) SELECT id, amount FROM public.staging",
                ["db1.public.staging", "db1.public.orders"],
                [],
                {},
            ),
            ("DELETE FROM public.logs WHERE id=1", [], ["db1.public.logs"], {}),
            (
                "SELECT o.id, c.name FROM public.orders o JOIN public.customers c ON o.customer_id = c.id",
                ["db1.public.orders", "db1.public.customers"],
                [],
                {},
            ),
            (
                "UPDATE public.customers SET name='x' WHERE id IN (SELECT id FROM public.staging)",
                ["db1.public.customers", "db1.public.staging"],
                [],
                {},
            ),
            (
                ["INSERT INTO public.t1 SELECT * FROM public.t2;", "DELETE FROM public.t3 WHERE id=1;"],
                ["db1.public.t1", "db1.public.t2", "db1.public.t3"],
                [],
                {},
            ),
            ("SELECT id, amount FROM myschema.orders", ["db1.myschema.orders"], [], {}),
        ],
    )
    def test_spannerquerydatabaseinstanceoperator_get_openlineage_facets(
        self, sql, expected_inputs, expected_outputs, expected_lineage
    ):
        # Arrange
        class SpannerHookForTests(DbApiHook):
            conn_name_attr = "gcp_conn_id"
            get_conn = MagicMock(name="conn")
            get_connection = MagicMock()
            database = DB_ID

            def get_openlineage_database_info(self, connection):
                return DatabaseInfo(
                    scheme="spanner",
                    authority=f"{PROJECT_ID}/{INSTANCE_ID}",
                    database=DB_ID,
                    information_schema_columns=[
                        "table_schema",
                        "table_name",
                        "column_name",
                        "ordinal_position",
                        "spanner_type",
                    ],
                    information_schema_table_name="information_schema.columns",
                    use_flat_cross_db_query=False,
                    is_information_schema_cross_db=False,
                    is_uppercase_names=False,
                )

        dbapi_hook = SpannerHookForTests()

        class SpannerOperatorForTest(SpannerQueryDatabaseInstanceOperator):
            @property
            def hook(self):
                return dbapi_hook

        op = SpannerOperatorForTest(
            task_id=TASK_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            gcp_conn_id="spanner_conn",
            query=sql,
        )

        dbapi_hook.get_connection.return_value = Connection(
            conn_id="spanner_conn", conn_type="spanner", host="spanner-host"
        )

        combined_rows = []
        for ds in expected_inputs + expected_outputs:
            tbl = ds.split(".", 1)[1]
            combined_rows.extend(SCHEMA_ROWS.get(tbl, []))

        dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [combined_rows, []]

        # Act
        lineage = op.get_openlineage_facets_on_complete(task_instance=None)
        assert lineage is not None

        # Assert inputs
        input_names = {ds.name for ds in lineage.inputs}
        assert input_names == set(expected_inputs)
        for ds in lineage.inputs:
            assert ds.namespace == f"spanner://{PROJECT_ID}/{INSTANCE_ID}"

        # Assert outputs
        output_names = {ds.name for ds in lineage.outputs}
        assert output_names == set(expected_outputs)
        for ds in lineage.outputs:
            assert ds.namespace == f"spanner://{PROJECT_ID}/{INSTANCE_ID}"

        # Assert SQLJobFacet
        sql_job = lineage.job_facets["sql"]
        if isinstance(sql, list):
            for q in sql:
                assert q.replace(";", "").strip() in sql_job.query.replace(";", "")
        else:
            assert sql_job.query == sql

        # Assert column lineage
        found_lineage = {
            getattr(field, "field", None) or getattr(field, "name", None): [
                f"{inp.dataset.name}.{getattr(inp, 'field', getattr(inp, 'name', None))}"
                for inp in getattr(field, "inputFields", [])
            ]
            for ds in lineage.outputs + lineage.inputs
            for cl_facet in [ds.facets.get("columnLineage")]
            if cl_facet
            for field in cl_facet.fields
        }

        for col, sources in expected_lineage.items():
            assert col in found_lineage
            for src in sources:
                assert any(src in s for s in found_lineage[col])

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_create(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeployDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, ddl_statements=DDL_STATEMENTS
        )
        mock_hook.return_value.update_database.assert_not_called()
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_create_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeployDatabaseInstanceOperator(
            instance_id=INSTANCE_ID, database_id=DB_ID, ddl_statements=DDL_STATEMENTS, task_id="id"
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID, ddl_statements=DDL_STATEMENTS
        )
        mock_hook.return_value.update_database.assert_not_called()
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_create_with_pre_existing_db(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeployDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_not_called()
        mock_hook.return_value.update_database.assert_not_called()
        assert result

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "database_id", "ddl_statements", "exp_msg"),
        [
            ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, "project_id"),
            (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, "instance_id"),
            (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, "database_id"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_create_ex_if_param_missing(
        self, mock_hook, project_id, instance_id, database_id, ddl_statements, exp_msg
    ):
        with pytest.raises(AirflowException) as ctx:
            SpannerDeployDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id",
            )
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_update(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerUpdateDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id",
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.update_database.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            operation_id=None,
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_update_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerUpdateDatabaseInstanceOperator(
            instance_id=INSTANCE_ID, database_id=DB_ID, ddl_statements=DDL_STATEMENTS, task_id="id"
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.update_database.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            operation_id=None,
        )
        assert result

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "database_id", "ddl_statements", "exp_msg"),
        [
            ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, "project_id"),
            (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, "instance_id"),
            (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, "database_id"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_update_ex_if_param_missing(
        self, mock_hook, project_id, instance_id, database_id, ddl_statements, exp_msg
    ):
        with pytest.raises(AirflowException) as ctx:
            SpannerUpdateDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id",
            )
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_update_ex_if_database_not_exist(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerUpdateDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id",
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert (
            "The Cloud Spanner database 'db1' in project 'project-id' and "
            "instance 'instance-id' is missing" in str(err)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_delete(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeleteDatabaseInstanceOperator(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_delete_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeleteDatabaseInstanceOperator(instance_id=INSTANCE_ID, database_id=DB_ID, task_id="id")
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_delete_exits_and_succeeds_if_database_does_not_exist(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeleteDatabaseInstanceOperator(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_not_called()
        assert result

    @pytest.mark.parametrize(
        ("project_id", "instance_id", "database_id", "ddl_statements", "exp_msg"),
        [
            ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, "project_id"),
            (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, "instance_id"),
            (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, "database_id"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.spanner.SpannerHook")
    def test_database_delete_ex_if_param_missing(
        self, mock_hook, project_id, instance_id, database_id, ddl_statements, exp_msg
    ):
        with pytest.raises(AirflowException) as ctx:
            SpannerDeleteDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id",
            )
        err = ctx.value
        assert f"The required parameter '{exp_msg}' is empty" in str(err)
        mock_hook.assert_not_called()
