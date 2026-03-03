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

from airflow.providers.common.ai.operators.llm_schema_compare import (
    LLMSchemaCompareOperator,
    SchemaCompareResult,
)
from airflow.providers.common.sql.config import DataSourceConfig
from airflow.providers.common.sql.hooks.sql import DbApiHook


def _make_ds_config(conn_id="test_conn", table_name="test_table", **kwargs):
    """Create a mock DataSourceConfig that bypasses __post_init__ validation."""
    ds = MagicMock(spec=DataSourceConfig)
    ds.conn_id = conn_id
    ds.table_name = table_name
    ds.format = kwargs.get("format", "parquet")
    for k, v in kwargs.items():
        setattr(ds, k, v)
    return ds


@pytest.fixture
def db_hook():
    hook = MagicMock(spec=DbApiHook)
    hook.get_table_schema.return_value = [
        {"name": "id", "type": "INTEGER"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "status", "type": "TEXT"},
    ]
    hook.dialect.get_primary_keys.return_value = ["id"]
    hook.inspector.get_foreign_keys.return_value = [
        {"constrained_columns": ["customer_id"], "referred_table": "customers", "referred_columns": ["id"]}
    ]
    hook.inspector.get_indexes.return_value = [
        {"name": "idx_orders_status", "column_names": ["status"], "unique": False}
    ]
    return hook


class TestLLMSchemaCompareOperator:
    def test_init_raises_value_error_if_no_sources(self):
        with pytest.raises(ValueError, match="Provide at least one of 'data_sources' or 'db_conn_ids'"):
            LLMSchemaCompareOperator(task_id="test_task", prompt="test prompt", llm_conn_id="llm_conn")

    def test_init_raises_value_error_if_db_conn_ids_without_table_names(self):
        with pytest.raises(ValueError, match="'table_names' is required when using 'db_conn_ids'"):
            LLMSchemaCompareOperator(
                task_id="test_task", prompt="test prompt", db_conn_ids=["test_conn"], llm_conn_id="llm_conn"
            )

    def test_init_succeeds_with_data_sources(self):
        ds_config = _make_ds_config()
        op = LLMSchemaCompareOperator(
            task_id="test_task", prompt="test prompt", data_sources=[ds_config], llm_conn_id="llm_conn"
        )
        assert op.data_sources == [ds_config]
        assert op.db_conn_ids == []
        assert op.table_names == []
        assert op.context_strategy == "full"
        assert op.reasoning_mode is True

    def test_init_succeeds_with_db_conn_ids_and_table_names(self):
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test prompt",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        assert op.data_sources == []
        assert op.db_conn_ids == ["test_conn"]
        assert op.table_names == ["test_table"]
        assert op.context_strategy == "full"
        assert op.reasoning_mode is True

    def test_init_succeeds_with_all_parameters(self):
        ds_config = _make_ds_config(conn_id="test_conn_ds", table_name="test_table_ds")
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test prompt",
            data_sources=[ds_config],
            db_conn_ids=["test_conn_db"],
            table_names=["test_table_db"],
            context_strategy=None,
            reasoning_mode=False,
            system_prompt="additional instructions",
            agent_params={"temperature": 0.5},
            llm_conn_id="llm_conn",
        )
        assert op.data_sources == [ds_config]
        assert op.db_conn_ids == ["test_conn_db"]
        assert op.table_names == ["test_table_db"]
        assert op.context_strategy is None
        assert op.reasoning_mode is False
        assert op.system_prompt == "additional instructions"
        assert op.agent_params == {"temperature": 0.5}

    @mock.patch("airflow.providers.common.ai.operators.llm_schema_compare.BaseHook")
    def test_get_db_hook_success(self, mock_base_hook):
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        mock_hook = MagicMock(spec=DbApiHook)
        mock_base_hook.get_connection.return_value.get_hook.return_value = mock_hook
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        result = op._get_db_hook("test_conn")
        assert result == mock_hook
        mock_base_hook.get_connection.assert_called_once_with("test_conn")

    @mock.patch("airflow.providers.common.ai.operators.llm_schema_compare.BaseHook")
    def test_get_db_hook_raises_value_error_for_non_dbapi_hook(self, mock_base_hook):
        mock_non_dbapi_hook = mock.Mock()
        mock_non_dbapi_hook.__class__.__name__ = "NonDbApiHook"
        mock_base_hook.get_connection.return_value.get_hook.return_value = mock_non_dbapi_hook
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        with pytest.raises(ValueError, match="does not provide a DbApiHook"):
            op._get_db_hook("test_conn")

    @mock.patch("airflow.providers.common.ai.operators.llm_schema_compare.BaseHook")
    def test_is_dbapi_connection_returns_true(self, mock_base_hook):
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        mock_hook = MagicMock(spec=DbApiHook)
        mock_base_hook.get_connection.return_value.get_hook.return_value = mock_hook
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        result = op._is_dbapi_connection("test_conn")
        assert result is True

    @mock.patch("airflow.providers.common.ai.operators.llm_schema_compare.BaseHook")
    def test_is_dbapi_connection_returns_false_for_non_dbapi_hook(self, mock_base_hook):
        mock_non_dbapi_hook = mock.Mock()
        mock_base_hook.get_connection.return_value.get_hook.return_value = mock_non_dbapi_hook
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        result = op._is_dbapi_connection("test_conn")
        assert result is False

    @mock.patch("airflow.providers.common.ai.operators.llm_schema_compare.BaseHook")
    def test_is_dbapi_connection_returns_false_on_exception(self, mock_base_hook):
        mock_base_hook.get_connection.side_effect = Exception("Connection error")
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["test_conn"],
            table_names=["test_table"],
            llm_conn_id="llm_conn",
        )
        result = op._is_dbapi_connection("test_conn")
        assert result is False

    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._get_db_hook"
    )
    def test_db_hooks_cached_property(self, mock_get_db_hook):
        mock_hook1 = mock.Mock()
        mock_hook2 = mock.Mock()
        mock_get_db_hook.side_effect = [mock_hook1, mock_hook2]

        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["conn1", "conn2"],
            table_names=["table"],
            llm_conn_id="llm_conn",
        )

        hooks = op._db_hooks
        assert hooks == {"conn1": mock_hook1, "conn2": mock_hook2}
        assert mock_get_db_hook.call_count == 2

        hooks_again = op._db_hooks
        assert hooks_again == {"conn1": mock_hook1, "conn2": mock_hook2}
        assert mock_get_db_hook.call_count == 2

    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._introspect_db_schema"
    )
    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._introspect_datasource_schema"
    )
    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._get_db_hook"
    )
    def test_build_schema_context(self, mock_get_db_hook, mock_introspect_ds, mock_introspect_db):
        mock_introspect_db.return_value = "db_schema"
        mock_introspect_ds.return_value = "ds_schema"

        mock_hook = mock.Mock(dialect_name="test_dialect")
        mock_get_db_hook.return_value = mock_hook

        ds_config = _make_ds_config(conn_id="ds_conn", table_name="ds_table")
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["db_conn"],
            table_names=["db_table"],
            data_sources=[ds_config],
            llm_conn_id="llm_conn",
        )

        result = op._build_schema_context()

        assert "Source: db_conn (test_dialect)" in result
        assert "Table: db_table" in result
        assert "db_schema" in result
        assert "ds_schema" in result
        mock_introspect_db.assert_called_once_with(mock_hook, "db_table")
        mock_introspect_ds.assert_called_once_with(ds_config)

    def test_build_system_prompt(self):
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="test",
            db_conn_ids=["conn"],
            table_names=["table"],
            system_prompt="extra instructions",
            reasoning_mode=True,
            llm_conn_id="llm_conn",
        )
        schema_context = "schema info"
        prompt = op._build_system_prompt(schema_context)

        assert "You are a database schema comparison expert." in prompt
        assert "Consider cross-system type equivalences:" in prompt
        assert "Schemas to compare:\n\nschema info" in prompt
        assert "Additional instructions:\nextra instructions" in prompt

    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._build_schema_context"
    )
    @mock.patch(
        "airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator._build_system_prompt"
    )
    def test_execute(self, mock_build_system_prompt, mock_build_schema_context):
        mock_build_schema_context.return_value = "schema_context"
        mock_build_system_prompt.return_value = "system_prompt"

        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="user_prompt",
            db_conn_ids=["conn"],
            table_names=["table"],
            llm_conn_id="llm_conn",
            agent_params={"param": "value"},
        )

        mock_llm_hook = mock.Mock()
        mock_agent = mock.Mock()
        mock_agent.run_sync.return_value.output = SchemaCompareResult(
            compatible=True, mismatches=[], summary="All good"
        )
        mock_llm_hook.create_agent.return_value = mock_agent
        op.llm_hook = mock_llm_hook

        result = op.execute(context={})

        mock_build_schema_context.assert_called_once()
        mock_build_system_prompt.assert_called_once_with("schema_context")
        mock_llm_hook.create_agent.assert_called_once_with(
            output_type=SchemaCompareResult,
            instructions="system_prompt",
            param="value",
        )
        mock_agent.run_sync.assert_called_once_with("user_prompt")
        assert result == {"compatible": True, "mismatches": [], "summary": "All good"}

    def test_introspect_full_schema(self, db_hook):
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="user_prompt",
            db_conn_ids=["conn"],
            table_names=["table"],
            llm_conn_id="llm_conn",
            agent_params={"param": "value"},
        )
        result = op._introspect_db_schema(db_hook, "orders")

        assert "customer_id" in result
        assert "Primary Key: id" in result
        assert "Foreign Key: (customer_id) -> customers(id)" in result
        assert "Index: idx_orders_status (status)" in result

    def test_introspect_empty_table_returns_empty_string(self, db_hook):
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="user_prompt",
            db_conn_ids=["conn"],
            table_names=["wrong_table"],
            llm_conn_id="llm_conn",
            agent_params={"param": "value"},
        )
        db_hook.get_table_schema.return_value = []

        result = op._introspect_db_schema(db_hook, "wrong_table")

        assert result == ""

    def test_introspect_basic_strategy_omits_constraints(self, db_hook):
        op = LLMSchemaCompareOperator(
            task_id="test_task",
            prompt="user_prompt",
            db_conn_ids=["conn"],
            table_names=["wrong_table"],
            llm_conn_id="llm_conn",
            agent_params={"param": "value"},
            context_strategy=None,
        )

        result = op._introspect_db_schema(db_hook, "orders")

        assert result.startswith("Columns:")
        assert "Primary Key" not in result
        assert "Foreign Key" not in result
        assert "Index" not in result
