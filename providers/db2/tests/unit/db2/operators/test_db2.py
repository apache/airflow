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

"""Unit tests for Db2Operator."""

from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.db2.operators.db2 import Db2Operator


class TestDb2Operator:
    """Test Db2Operator functionality."""

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_execute_simple_query(self, mock_hook_class):
        """Test executing a simple SELECT query."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]

        operator = Db2Operator(
            task_id="test_query",
            sql="SELECT * FROM EMPLOYEES",
            db2_conn_id="db2_default",
        )

        result = operator.execute(context={})

        assert result == [(1, "John"), (2, "Jane")]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM EMPLOYEES", None)
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_execute_with_parameters(self, mock_hook_class):
        """Test executing query with parameters."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        operator = Db2Operator(
            task_id="test_insert",
            sql="INSERT INTO EMPLOYEES (ID, NAME) VALUES (?, ?)",
            parameters=(1, "John Doe"),
            db2_conn_id="db2_default",
        )

        operator.execute(context={})

        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO EMPLOYEES (ID, NAME) VALUES (?, ?)", (1, "John Doe")
        )
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_execute_with_autocommit(self, mock_hook_class):
        """Test executing with autocommit enabled."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        operator = Db2Operator(
            task_id="test_autocommit",
            sql="UPDATE EMPLOYEES SET SALARY = 50000 WHERE ID = 1",
            autocommit=True,
            db2_conn_id="db2_default",
        )

        operator.execute(context={})

        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_execute_split_statements(self, mock_hook_class):
        """Test executing multiple statements split by semicolon."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [[(1,)], [(2,)]]

        operator = Db2Operator(
            task_id="test_split",
            sql="SELECT 1 FROM SYSIBM.SYSDUMMY1; SELECT 2 FROM SYSIBM.SYSDUMMY1",
            split_statements=True,
            db2_conn_id="db2_default",
        )

        result = operator.execute(context={})

        assert result == [(2,)]
        assert mock_cursor.execute.call_count == 2
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_execute_error_handling(self, mock_hook_class):
        """Test error handling and rollback."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("SQL Error")

        operator = Db2Operator(
            task_id="test_error",
            sql="INVALID SQL",
            db2_conn_id="db2_default",
        )

        with pytest.raises(Exception, match="SQL Error"):
            operator.execute(context={})

        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("airflow.providers.db2.operators.db2.Db2Hook")
    def test_on_kill(self, mock_hook_class):
        """Test cleanup on task kill."""
        mock_hook = mock_hook_class.return_value
        mock_conn = mock.MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        operator = Db2Operator(
            task_id="test_kill",
            sql="SELECT * FROM EMPLOYEES",
            db2_conn_id="db2_default",
        )
        operator.hook = mock_hook

        operator.on_kill()

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_template_fields(self):
        """Test that sql is a template field."""
        operator = Db2Operator(
            task_id="test_template",
            sql="SELECT * FROM EMPLOYEES WHERE DATE = '{{ ds }}'",
            db2_conn_id="db2_default",
        )

        assert "sql" in operator.template_fields
        assert operator.sql == "SELECT * FROM EMPLOYEES WHERE DATE = '{{ ds }}'"

    def test_template_ext(self):
        """Test that .sql files are recognized as templates."""
        operator = Db2Operator(
            task_id="test_template_ext",
            sql="query.sql",
            db2_conn_id="db2_default",
        )

        assert ".sql" in operator.template_ext
