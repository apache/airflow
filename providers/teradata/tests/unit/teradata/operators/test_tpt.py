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
- TdLoadOperator: For data transfers between files and tables
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest

from airflow.providers.teradata.hooks.tpt import TptHook
from airflow.providers.teradata.operators.tpt import DdlOperator, TdLoadOperator


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


class TestTdLoadOperator:
    """
    Tests for TdLoadOperator.

    This test suite validates the TdLoadOperator functionality across different modes:
    - file_to_table: Loading data from a file to a Teradata table
    - table_to_file: Exporting data from a Teradata table to a file
    - select_stmt_to_file: Exporting data from a SQL SELECT statement to a file
    - table_to_table: Transferring data between two Teradata tables

    It also tests parameter validation, error handling, templating, and resource cleanup.
    """

    def setup_method(self, method):
        # No MagicMock connections needed; use only string conn_ids in tests
        pass

    # ----- Tests for Basic Operation Modes -----

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_file_to_table_mode(self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn):
        """Test loading data from a file to a Teradata table (with connection and job var patching)"""
        # Create operator
        operator = TdLoadOperator(
            task_id="test_file_to_table",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute operator
        result = operator.execute({})

        # Assertions
        assert result == 0
        mock_execute_tdload.assert_called_once()
        mock_prepare_job_var.assert_called_once()
        mock_get_conn.assert_called()
        # Verify that the operator initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_file_to_table_with_default_target_conn(
        self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn
    ):
        """Test file to table loading with default target connection"""
        operator = TdLoadOperator(
            task_id="test_file_to_table_default_target",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            # No target_teradata_conn_id - should default to teradata_conn_id
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that target_teradata_conn_id was set to teradata_conn_id
        assert operator.target_teradata_conn_id == "teradata_default"
        # Verify that hooks were initialized
        assert operator._src_hook is not None
        assert operator._dest_hook is not None
        mock_execute_tdload.assert_called_once()
        mock_prepare_job_var.assert_called_once()
        mock_get_conn.assert_called()

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_table_to_file_mode(self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn):
        """Test exporting data from a Teradata table to a file"""
        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_table_to_file",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hooks were initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is None  # No destination hook for table_to_file
        mock_execute_tdload.assert_called_once()
        mock_prepare_job_var.assert_called_once()
        mock_get_conn.assert_called()

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_select_stmt_to_file_mode(self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn):
        """Test exporting data from a SELECT statement to a file"""
        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_select_to_file",
            select_stmt="SELECT * FROM source_db.source_table WHERE id > 1000",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_default",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that hooks were initialized correctly
        assert operator._src_hook is not None
        assert operator._dest_hook is None  # No destination hook for select_to_file
        mock_execute_tdload.assert_called_once()
        mock_prepare_job_var.assert_called_once()
        mock_get_conn.assert_called()

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_table_to_table_mode(self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn):
        """Test transferring data between two Teradata tables"""
        # Configure the operator
        operator = TdLoadOperator(
            task_id="test_table_to_table",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    # ----- Tests for Advanced Operation Modes -----

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_file_to_table_with_insert_stmt(self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn):
        """Test loading from file to table with custom INSERT statement"""
        # Configure the operator with custom INSERT statement
        operator = TdLoadOperator(
            task_id="test_file_to_table_with_insert",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            insert_stmt="INSERT INTO target_db.target_table (col1, col2) VALUES (?, ?)",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized for file_to_table mode
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        return_value={"host": "mock_host", "login": "mock_user", "password": "mock_pass"},
    )
    @patch(
        "airflow.providers.teradata.operators.tpt.prepare_tdload_job_var_file",
        return_value="dummy job var content",
    )
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.execute_tdload", return_value=0)
    def test_table_to_table_with_select_and_insert(
        self, mock_execute_tdload, mock_prepare_job_var, mock_get_conn
    ):
        """Test transferring data between tables with custom SELECT and INSERT statements"""
        # Configure the operator with custom SELECT and INSERT statements
        operator = TdLoadOperator(
            task_id="test_table_to_table_with_select_insert",
            select_stmt="SELECT col1, col2 FROM source_db.source_table WHERE col3 > 1000",
            target_table="target_db.target_table",
            insert_stmt="INSERT INTO target_db.target_table (col1, col2) VALUES (?, ?)",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Execute the operator
        result = operator.execute({})

        # Verify the results
        assert result == 0
        # Verify that both hooks were initialized for table_to_table mode
        assert operator._src_hook is not None
        assert operator._dest_hook is not None

    # ----- Parameter Validation Tests -----

    def test_invalid_parameter_combinations(self):
        """Test validation of invalid parameter combinations"""
        # Test 1: Missing both source and target parameters
        with pytest.raises(ValueError, match="Invalid parameter combination"):
            TdLoadOperator(
                task_id="test_invalid_params",
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test 2: Missing target_teradata_conn_id for table_to_table mode
        with pytest.raises(ValueError, match="target_teradata_conn_id must be provided"):
            TdLoadOperator(
                task_id="test_missing_target_conn",
                source_table="source_db.source_table",
                target_table="target_db.target_table",
                teradata_conn_id="teradata_default",
                # Missing target_teradata_conn_id for table_to_table
            ).execute({})

        # Test 3: Both source_table and select_stmt provided (contradictory sources)
        with pytest.raises(
            ValueError, match="Both source_table and select_stmt cannot be provided simultaneously"
        ):
            TdLoadOperator(
                task_id="test_both_source_and_select",
                source_table="source_db.table",
                select_stmt="SELECT * FROM other_db.table",
                target_file_name="/path/to/export.csv",
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test 4: insert_stmt without target_table
        with pytest.raises(ValueError, match="insert_stmt is provided but target_table is not specified"):
            TdLoadOperator(
                task_id="test_insert_stmt_no_target",
                source_file_name="/path/to/source.csv",
                insert_stmt="INSERT INTO mytable VALUES (?, ?)",
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test 5: Only target_file_name provided (no source)
        with pytest.raises(ValueError, match="Invalid parameter combination"):
            TdLoadOperator(
                task_id="test_no_source_with_target_file",
                target_file_name="/path/to/file.csv",
                teradata_conn_id="teradata_default",
            ).execute({})

        # Test 6: Only source_file_name provided (no target)
        with pytest.raises(ValueError, match="Invalid parameter combination"):
            TdLoadOperator(
                task_id="test_source_file_no_target_table",
                source_file_name="/path/to/source.csv",
                teradata_conn_id="teradata_default",
            ).execute({})

    # ----- Error Handling Tests -----

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        side_effect=RuntimeError("Connection not found"),
    )
    def test_error_handling_execute_tdload(self, mock_get_conn):
        """Test error handling with invalid connection ID"""
        operator = TdLoadOperator(
            task_id="test_error_handling",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="nonexistent_connection",
            target_teradata_conn_id="teradata_target",
        )
        with pytest.raises((RuntimeError, ValueError, KeyError)):
            operator.execute({})

    @patch(
        "airflow.providers.teradata.hooks.ttu.TtuHook.get_conn",
        side_effect=RuntimeError("Connection not found"),
    )
    def test_error_handling_get_conn(self, mock_get_conn):
        """Test error handling with invalid target connection ID"""
        operator = TdLoadOperator(
            task_id="test_error_handling_conn",
            source_file_name="/path/to/data.csv",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="nonexistent_target_connection",
        )
        with pytest.raises((RuntimeError, ValueError, KeyError)):
            operator.execute({})

    # ----- Resource Cleanup Tests -----

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_on_kill(self, mock_ssh_hook, mock_tpt_hook):
        """Test on_kill method cleans up resources properly"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Set up hooks manually
        operator._src_hook = MagicMock()
        operator._dest_hook = MagicMock()

        # Call on_kill
        operator.on_kill()

        # Verify hooks were cleaned up
        operator._src_hook.on_kill.assert_called_once()
        operator._dest_hook.on_kill.assert_called_once()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    def test_on_kill_no_hooks(self, mock_tpt_hook):
        """Test on_kill method when no hooks are initialized"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill_no_hooks",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )

        # Set hooks to None
        operator._src_hook = None
        operator._dest_hook = None

        # Call on_kill (should not raise any exceptions)
        operator.on_kill()

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_on_kill_with_only_src_hook(self, mock_ssh_hook, mock_tpt_hook):
        """Test on_kill with only source hook initialized"""
        # Set up operator
        operator = TdLoadOperator(
            task_id="test_on_kill_src_only",
            source_table="source_db.source_table",
            target_file_name="/path/to/export.csv",  # table_to_file mode
            teradata_conn_id="teradata_default",
        )

        # Set up only source hook
        operator._src_hook = MagicMock()
        operator._dest_hook = None

        # Call on_kill
        operator.on_kill()

        # Verify source hook was cleaned up
        operator._src_hook.on_kill.assert_called_once()

    # ----- Job Variable File Tests -----

    @patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True)
    @patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content")
    def test_with_local_job_var_file(self, mock_read_file, mock_is_valid_file):
        """Test using a local job variable file"""
        # Configure operator with only job var file (no source/target parameters needed)
        operator = TdLoadOperator(
            task_id="test_with_job_var_file",
            tdload_job_var_file="/path/to/job_vars.txt",
            teradata_conn_id="teradata_default",
        )

        # Execute
        result = operator.execute({})

        # Verify the execution was successful (returns 0 for success)
        assert result == 0

    @patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True)
    @patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content")
    def test_with_local_job_var_file_and_options(self, mock_read_file, mock_is_valid_file):
        """Test using a local job variable file with additional tdload options"""
        # Set up mocks
        with patch("airflow.providers.teradata.hooks.tpt.TptHook") as mock_tpt_hook:
            mock_tpt_hook_instance = mock_tpt_hook.return_value
            mock_tpt_hook_instance._execute_tdload_locally.return_value = 0

        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True),
            patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content"),
        ):
            # Configure operator with job var file and additional options
            operator = TdLoadOperator(
                task_id="test_with_job_var_file_and_options",
                tdload_job_var_file="/path/to/job_vars.txt",
                tdload_options="-v -u",  # Add verbose and Unicode options
                tdload_job_name="custom_job_name",
                teradata_conn_id="teradata_default",
            )

            # Execute
            result = operator.execute({})

            # Verify the execution was successful (returns 0 for success)
            assert result == 0

    @patch("airflow.providers.teradata.hooks.tpt.TptHook")
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    def test_with_invalid_local_job_var_file(self, mock_ssh_hook, mock_tpt_hook):
        """Test with invalid local job variable file path"""
        # Set up mocks
        with patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=False):
            # Configure operator
            operator = TdLoadOperator(
                task_id="test_with_invalid_job_var_file",
                tdload_job_var_file="/path/to/nonexistent_file.txt",
                teradata_conn_id="teradata_default",
            )

            # Execute and check for exception
            with pytest.raises(ValueError, match="is invalid or does not exist"):
                operator.execute({})

    # ----- Specific subprocess mocking tests -----

    @patch("airflow.providers.teradata.hooks.tpt.subprocess.Popen")
    @patch("airflow.providers.teradata.hooks.tpt.shutil.which")
    @patch("airflow.models.Connection")
    def test_direct_tdload_execution_mocking(self, mock_conn, mock_which, mock_popen):
        """Test the direct execution of tdload with proper mocking"""
        # Ensure the binary is found
        mock_which.return_value = "/usr/bin/tdload"

        # Mock the subprocess
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = MagicMock()
        mock_process.stdout.readline.side_effect = [
            b"Starting TDLOAD...\n",
            b"Processing data...\n",
            b"1000 rows loaded successfully\n",
            b"",
        ]
        mock_popen.return_value = mock_process

        # Create the TPT hook directly
        hook = TptHook(teradata_conn_id="teradata_default")

        # Execute the command directly
        result = hook._execute_tdload_locally(
            job_var_content="DEFINE JOB sample_job;\nUSING OPERATOR sel;\nSELECT * FROM source_table;\n",
            tdload_options="-v",
            tdload_job_name="sample_job",
        )

        # Verify the result
        assert result == 0
        mock_popen.assert_called_once()
        cmd_args = mock_popen.call_args[0][0]
        assert cmd_args[0] == "tdload"
        assert "-j" in cmd_args
        assert "-v" in cmd_args
        assert "sample_job" in cmd_args

    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook._execute_tdload_locally")
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    @patch("airflow.models.Connection")
    def test_execute_with_local_job_var_file_direct_patch(
        self, mock_conn, mock_hook_init, mock_execute_local, mock_dest_hook, mock_src_hook
    ):
        """Test TdLoadOperator with a local job var file using direct patching (bteq style)"""
        # Arrange
        mock_execute_local.return_value = 0
        operator = TdLoadOperator(
            task_id="test_with_local_job_var_file_direct_patch",
            tdload_job_var_file="/path/to/job_vars.txt",
            teradata_conn_id="teradata_default",
        )
        # Manually set hooks since we bypassed __init__
        operator._src_hook = mock_src_hook
        operator._dest_hook = mock_dest_hook
        operator._src_hook._execute_tdload_locally = mock_execute_local
        # Patch file validation and reading
        with (
            patch("airflow.providers.teradata.operators.tpt.is_valid_file", return_value=True),
            patch("airflow.providers.teradata.operators.tpt.read_file", return_value="job var content"),
        ):
            # Act
            result = operator.execute({})
        # Assert
        mock_execute_local.assert_called_once_with("job var content", None, None)
        assert result == 0

    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    @patch("airflow.models.Connection")
    def test_on_kill_direct_patch(self, mock_conn, mock_hook_init, mock_dest_hook, mock_src_hook):
        """Test on_kill method with direct patching (bteq style)"""
        operator = TdLoadOperator(
            task_id="test_on_kill_direct_patch",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )
        # Set up mocked attributes
        operator._src_hook = mock_src_hook
        operator._dest_hook = mock_dest_hook
        # Ensure the mocked hooks have on_kill methods
        mock_src_hook.on_kill = Mock()
        mock_dest_hook.on_kill = Mock()

        # Act
        operator.on_kill()

        # Assert
        mock_src_hook.on_kill.assert_called_once()
        mock_dest_hook.on_kill.assert_called_once()

    @patch("airflow.providers.ssh.hooks.ssh.SSHHook")
    @patch.object(TdLoadOperator, "_src_hook", create=True)
    @patch.object(TdLoadOperator, "_dest_hook", create=True)
    @patch("airflow.providers.teradata.hooks.tpt.TptHook.__init__", return_value=None)
    @patch("airflow.models.Connection")
    def test_on_kill_no_hooks_direct_patch(
        self, mock_conn, mock_hook_init, mock_dest_hook, mock_src_hook, mock_ssh_hook
    ):
        """Test on_kill method when no hooks are initialized (bteq style)"""
        operator = TdLoadOperator(
            task_id="test_on_kill_no_hooks_direct_patch",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_default",
            target_teradata_conn_id="teradata_target",
        )
        operator._src_hook = None
        operator._dest_hook = None
        # Act
        operator.on_kill()
