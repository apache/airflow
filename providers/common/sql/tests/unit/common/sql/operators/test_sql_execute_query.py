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

try:
    import sqlglot
except ImportError:
    sqlglot = None  # type: ignore[assignment]

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.utils.sql_validation import SQLValidationError

needs_sqlglot = pytest.mark.skipif(sqlglot is None, reason="sqlglot is not installed")


class TestSQLExecuteQueryOperatorValidation:
    def test_defaults_disable_validation(self):
        op = SQLExecuteQueryOperator(task_id="test_op", sql="SELECT 1")
        assert op.validate_sql is False
        assert op.check_partition_filters is False
        assert op.partition_columns is None
        assert op.allow_unbounded is False
        assert op.dialect is None

    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_default_executes_without_validation(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(task_id="test_op", sql="SELECT * FROM table_without_filter")
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_validate_sql_success(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT id, name FROM users WHERE id = 1",
            validate_sql=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_validate_sql_invalid_syntax_fails_before_hook(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT TOP 10 * FROM users",
            validate_sql=True,
        )
        with pytest.raises(SQLValidationError, match="SQL syntax error"):
            op.execute(context={})

        mock_hook.run.assert_not_called()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_check_partition_filters_unbounded_fails_before_hook(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM big_lakehouse_table",
            check_partition_filters=True,
        )
        with pytest.raises(SQLValidationError, match="lacks a WHERE clause or partition filter"):
            op.execute(context={})

        mock_hook.run.assert_not_called()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_check_partition_filters_success(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM big_lakehouse_table WHERE ds = '2026-01-01'",
            check_partition_filters=True,
            partition_columns=["ds"],
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_check_partition_filters_allow_unbounded_with_limit(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM big_lakehouse_table LIMIT 10",
            check_partition_filters=True,
            allow_unbounded=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_check_partition_filters_cte_with_filter_succeeds(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="WITH cte AS (SELECT * FROM big_lakehouse_table WHERE ds = '2026-01-01') SELECT * FROM cte",
            check_partition_filters=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_check_partition_filters_missing_partition_col_fails(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM big_lakehouse_table WHERE other_col = 1",
            check_partition_filters=True,
            partition_columns=["ds"],
        )
        with pytest.raises(SQLValidationError, match="lacks required partition filter"):
            op.execute(context={})

        mock_hook.run.assert_not_called()

    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_missing_sqlglot_raises_feature_exception(self, mock_get_hook, monkeypatch):
        import airflow.providers.common.sql.utils.sql_validation as mod

        monkeypatch.setattr(mod, "sqlglot", None)

        mock_hook = mock.MagicMock()
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT 1",
            validate_sql=True,
        )
        with pytest.raises(AirflowOptionalProviderFeatureException, match="sqlglot is required"):
            op.execute(context={})

        mock_hook.run.assert_not_called()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_explicit_dialect_override(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT TOP 5 * FROM my_table WHERE id = 1",
            dialect="mssql",
            validate_sql=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_callable_dialect_on_hook(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = mock.MagicMock(return_value="postgres")
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT id FROM users WHERE id = 1",
            validate_sql=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_single_str_partition_column(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM events WHERE ds = '2026-01-01'",
            check_partition_filters=True,
            partition_columns="ds",
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()

    @needs_sqlglot
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_compound_union_allow_unbounded(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_hook.dialect_name = "postgres"
        mock_hook.run.return_value = [("result",)]
        mock_get_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(
            task_id="test_op",
            sql="SELECT * FROM tbl1 UNION ALL SELECT * FROM tbl2 LIMIT 10",
            check_partition_filters=True,
            allow_unbounded=True,
        )
        op.execute(context={})

        mock_hook.run.assert_called_once()
