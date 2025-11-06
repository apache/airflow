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
"""
Tests for Teradata Parallel Transporter (TPT) operators.

These tests validate the functionality of:
- DdlOperator: For DDL operations on Teradata databases
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.teradata.operators.tpt import DdlOperator


@pytest.fixture(autouse=True)
def patch_secure_delete():
    """Patch secure_delete for all tests to avoid ValueError from subprocess.run"""
    # Patch the reference used in hooks.tpt, not just tpt_util
    with patch("airflow.providers.teradata.hooks.tpt.secure_delete", return_value=None):
        yield


@pytest.fixture(autouse=True)
def patch_binary_checks():
    # Mock binary availability checks to prevent "binary not found" errors
    with patch("airflow.providers.teradata.hooks.tpt.shutil.which") as mock_which:
        mock_which.return_value = "/usr/bin/mock_binary"  # Always return a path
        yield


@pytest.fixture(autouse=True)
def patch_subprocess():
    # Mock subprocess calls to prevent actual binary execution
    with patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [b"Mock output\n", b""]
        mock_process.communicate.return_value = (b"Mock stdout", b"")
        mock_popen.return_value = mock_process
        yield


class TestDdlOperator:
    """
    Tests for DdlOperator.

    This test suite validates the DdlOperator functionality for:
    - Executing DDL statements on Teradata databases
    - Parameter validation
    - Error handling and error code management
    - Template rendering
    - Resource cleanup
    """

    def setup_method(self, method):
        # No MagicMock connections needed; use only string conn_ids in tests
        pass

    # ----- DDL Execution Tests -----

    @patch("airflow.providers.teradata.operators.tpt.TptHook")
    def test_ddl_execution(self, mock_tpt_hook):
        mock_hook_instance = mock_tpt_hook.return_value
        mock_hook_instance.get_conn.return_value = {
            "host": "mock_host",
            "login": "mock_user",
            "password": "mock_pass",
        }
        mock_hook_instance.execute_ddl.return_value = 0
        operator = DdlOperator(
            task_id="test_ddl",
            ddl=["CREATE TABLE test_db.test_table (id INT)", "CREATE INDEX idx ON test_db.test_table (id)"],
            teradata_conn_id="teradata_default",
            ddl_job_name="test_ddl_job",
        )
        result = operator.execute({})
        assert result == 0

    @patch("airflow.providers.teradata.operators.tpt.TptHook")
    def test_ddl_execution_with_multiple_statements(self, mock_tpt_hook):
        mock_hook_instance = mock_tpt_hook.return_value
        mock_hook_instance.get_conn.return_value = {
            "host": "mock_host",
            "login": "mock_user",
            "password": "mock_pass",
        }
        mock_hook_instance.execute_ddl.return_value = 0
        ddl_statements = [
            "CREATE TABLE test_db.customers (customer_id INTEGER, name VARCHAR(100), email VARCHAR(255))",
            "CREATE INDEX idx_customer_name ON test_db.customers (name)",
            """CREATE TABLE test_db.orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                FOREIGN KEY (customer_id) REFERENCES test_db.customers(customer_id)
            )""",
            "CREATE PROCEDURE test_db.get_customer(IN p_id INTEGER) BEGIN SELECT * FROM test_db.customers WHERE customer_id = p_id; END;",
        ]
        operator = DdlOperator(
            task_id="test_multiple_ddl",
            ddl=ddl_statements,
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0

    # ----- Parameter Validation Tests -----

    def test_ddl_parameter_validation(self):
        # Test empty DDL list
        with pytest.raises(ValueError, match="ddl parameter must be a non-empty list"):
            DdlOperator(
                task_id="test_empty_ddl",
                ddl=[],
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test non-list DDL parameter
        with pytest.raises(ValueError, match="ddl parameter must be a non-empty list"):
            DdlOperator(
                task_id="test_non_list_ddl",
                ddl="CREATE TABLE test_table (id INT)",  # string instead of list
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test DDL with empty string
        with pytest.raises(ValueError, match="ddl parameter must be a non-empty list"):
            DdlOperator(
                task_id="test_empty_string_ddl",
                ddl=["CREATE TABLE test_table (id INT)", ""],
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test DDL with None value
        with pytest.raises(ValueError, match="ddl parameter must be a non-empty list"):
            DdlOperator(
                task_id="test_none_ddl",
                ddl=None,
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test DDL with list containing non-string values
        with pytest.raises(ValueError, match="ddl parameter must be a non-empty list"):
            DdlOperator(
                task_id="test_non_string_ddl",
                ddl=["CREATE TABLE test_table (id INT)", 123],
                teradata_conn_id="teradata_default",
            ).execute({})

    @patch("airflow.providers.teradata.operators.tpt.TptHook")
    def test_error_list_validation(self, mock_tpt_hook):
        mock_hook_instance = mock_tpt_hook.return_value
        mock_hook_instance.get_conn.return_value = {
            "host": "mock_host",
            "login": "mock_user",
            "password": "mock_pass",
        }
        mock_hook_instance.execute_ddl.return_value = 0
        # Test with integer error code
        operator = DdlOperator(
            task_id="test_int_error_list",
            ddl=["CREATE TABLE test_table (id INT)"],
            error_list=3803,  # single integer
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0
        assert operator.error_list == 3803  # Original value should remain unchanged

        # Test with list of integers
        operator = DdlOperator(
            task_id="test_list_error_list",
            ddl=["CREATE TABLE test_table (id INT)"],
            error_list=[3803, 3807, 5495],  # list of integers
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0
        assert operator.error_list == [3803, 3807, 5495]

        # Test with invalid error_list type (string)
        with pytest.raises(ValueError, match="error_list must be an int or a list of ints"):
            DdlOperator(
                task_id="test_invalid_error_list_string",
                ddl=["CREATE TABLE test_table (id INT)"],
                error_list="3803",  # string instead of int or list
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test with invalid error_list type (dict)
        with pytest.raises(ValueError, match="error_list must be an int or a list of ints"):
            DdlOperator(
                task_id="test_invalid_error_list_dict",
                ddl=["CREATE TABLE test_table (id INT)"],
                error_list={"code": 3803},  # dict instead of int or list
                teradata_conn_id="teradata_default",
            ).execute({})

    # ----- Error Handling Tests -----

    @patch(
        "airflow.providers.teradata.hooks.tpt.TptHook.get_conn",
        side_effect=RuntimeError("Connection not found"),
    )
    def test_ddl_execution_with_error_handling(self, mock_get_conn):
        # Configure operator with error list
        operator = DdlOperator(
            task_id="test_ddl_with_errors",
            ddl=[
                "DROP TABLE test_db.nonexistent_table",  # This might generate error 3807 (object not found)
                "CREATE TABLE test_db.new_table (id INT)",
            ],
            error_list=[3807],  # Ignore "object does not exist" errors
            teradata_conn_id="teradata_default",
        )

        # Execute and verify RuntimeError is raised
        with pytest.raises(RuntimeError, match="Connection not found"):
            operator.execute({})

    @patch(
        "airflow.providers.teradata.hooks.tpt.TptHook.get_conn",
        side_effect=RuntimeError("Connection not found"),
    )
    def test_ddl_execution_error(self, mock_get_conn):
        # This test verifies the normal case since we can't easily simulate real DDL errors
        # In a real environment, DDL errors would be handled by the TPT hooks

        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_execution_error",
            ddl=["CREATE TABLE test_db.test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Execute and verify RuntimeError is raised
        with pytest.raises(RuntimeError, match="Connection not found"):
            operator.execute({})

    # ----- Resource Cleanup Tests -----

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_ddl_on_kill(self, mock_tpt_hook):
        # Set up mocks
        mock_hook_instance = mock_tpt_hook.return_value

        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_on_kill",
            ddl=["CREATE TABLE test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Set hook manually
        operator._hook = mock_hook_instance

        # Call on_kill
        operator.on_kill()

        # Verify hook was cleaned up
        mock_hook_instance.on_kill.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_ddl_on_kill_no_hook(self, mock_tpt_hook):
        # Configure operator
        operator = DdlOperator(
            task_id="test_ddl_on_kill_no_hook",
            ddl=["CREATE TABLE test_table (id INT)"],
            teradata_conn_id="teradata_default",
        )

        # Set hook to None
        operator._hook = None

        # Call on_kill (should not raise any exceptions)
        operator.on_kill()

    # ----- Templating Tests -----
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    @patch("airflow.providers.teradata.operators.tpt.TptHook")
    def test_template_ext(self, mock_tpt_hook, mock_ssh_hook):
        mock_hook_instance = mock_tpt_hook.return_value
        mock_hook_instance.get_conn.return_value = {
            "host": "mock_host",
            "login": "mock_user",
            "password": "mock_pass",
        }
        mock_hook_instance.execute_ddl.return_value = 0
        # Verify template_ext contains .sql
        assert ".sql" in DdlOperator.template_ext
        operator = DdlOperator(
            task_id="test_sql_file",
            ddl=["SELECT * FROM test_table;"],
            teradata_conn_id="teradata_default",
        )
        result = operator.execute({})
        assert result == 0

    # ----- SSL Connection Tests -----
    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch("airflow.models.Connection")
    def test_ddl_with_ssl_connection(self, mock_conn, mock_get_conn):
        """Test DDL operations with SSL-enabled Teradata connection"""
        operator = DdlOperator(
            task_id="test_ddl_with_ssl_connection",
            ddl=["CREATE TABLE test_table (id INT)"],
            teradata_conn_id="teradata_ssl",
        )
        result = operator.execute({})
        assert result == 0
        mock_get_conn.assert_called()
