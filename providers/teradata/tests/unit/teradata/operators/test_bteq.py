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
import tempfile
import unittest
from unittest import mock

import pytest

from airflow.providers.teradata.hooks.bteq import BteqHook
from airflow.providers.teradata.operators.bteq import BteqOperator

log = logging.getLogger(__name__)


class TestBteqOperator:
    @mock.patch.object(BteqHook, "execute_bteq_script")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute(self, mock_hook_init, mock_execute_bteq):
        task_id = "test_bteq_operator"
        sql = "SELECT * FROM my_table;"
        teradata_conn_id = "teradata_default"
        mock_context = {}
        # Given
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result
        operator = BteqOperator(
            task_id=task_id,
            sql=sql,
            teradata_conn_id=teradata_conn_id,
        )

        # When
        result = operator.execute(mock_context)

        # Then
        mock_hook_init.assert_called_once_with(teradata_conn_id=teradata_conn_id, ssh_conn_id=None)
        mock_execute_bteq.assert_called_once_with(sql + "\n.EXIT", None, "", 600, None, "", None, "UTF-8")
        assert result == "BTEQ execution result"

    @mock.patch.object(BteqHook, "execute_bteq_script")
    @mock.patch.object(BteqHook, "__init__", return_value=None)
    def test_execute_sql_only(self, mock_hook_init, mock_execute_bteq):
        # Arrange
        task_id = "test_bteq_operator"
        sql = "SELECT * FROM my_table;"
        teradata_conn_id = "teradata_default"
        mock_context = {}
        expected_result = "BTEQ execution result"
        mock_execute_bteq.return_value = expected_result

        operator = BteqOperator(
            task_id=task_id,
            sql=sql,
            teradata_conn_id=teradata_conn_id,
        )
        # Manually set _hook since we bypassed __init__
        operator._hook = mock.MagicMock()
        operator._hook.execute_bteq_script = mock_execute_bteq

        # Act
        result = operator.execute(mock_context)

        # Assert
        mock_hook_init.assert_called_once_with(teradata_conn_id=teradata_conn_id, ssh_conn_id=None)
        mock_execute_bteq.assert_called_once_with(
            sql + "\n.EXIT",  # Assuming the prepare_bteq_script_for_local_execution appends ".EXIT"
            None,  # default remote_working_dir
            "",  # bteq_script_encoding (default ASCII => empty string)
            600,  # timeout default
            None,  # timeout_rc
            "",  # bteq_session_encoding
            None,  # bteq_quit_rc
            "UTF-8",
        )
        assert result == expected_result

    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.execute_bteq_script")
    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.__init__", return_value=None)
    def test_execute_sql_local(self, mock_hook_init, mock_execute_script):
        sql = "SELECT * FROM test_table;"
        expected_result = 0
        mock_execute_script.return_value = expected_result
        context = {}

        op = BteqOperator(
            task_id="test_local_sql",
            sql=sql,
            teradata_conn_id="td_conn",
        )
        op._hook = mock.Mock()
        op._hook.execute_bteq_script = mock_execute_script

        result = op.execute(context)

        mock_hook_init.assert_called_once_with(teradata_conn_id="td_conn", ssh_conn_id=None)
        mock_execute_script.assert_called_once()
        assert result == expected_result

    @mock.patch.object(BteqHook, "on_kill")
    def test_on_kill(self, mock_on_kill):
        task_id = "test_bteq_operator"
        sql = "SELECT * FROM my_table;"
        # Given
        operator = BteqOperator(
            task_id=task_id,
            sql=sql,
        )
        operator._hook = BteqHook(None)

        # When
        operator.on_kill()

        # Then
        mock_on_kill.assert_called_once()

    def test_on_kill_not_initialized(self):
        task_id = "test_bteq_operator"
        sql = "SELECT * FROM my_table;"
        # Given
        operator = BteqOperator(
            task_id=task_id,
            sql=sql,
        )
        operator._hook = None

        # When/Then (no exception should be raised)
        operator.on_kill()

    def test_template_fields(self):
        # Verify template fields are defined correctly
        print(BteqOperator.template_fields)
        assert BteqOperator.template_fields == "sql"

    def test_execute_raises_if_no_sql_or_file(self):
        op = BteqOperator(task_id="fail_case", teradata_conn_id="td_conn")
        with pytest.raises(
            ValueError,
            match="Failed to execute BTEQ script due to missing required parameters: either 'sql' or 'file_path' must be provided.",
        ):
            op.execute({})

    @mock.patch("airflow.providers.teradata.operators.bteq.is_valid_file", return_value=False)
    def test_invalid_file_path(self, mock_is_valid_file):
        op = BteqOperator(
            task_id="fail_invalid_file",
            file_path="/invalid/path.sql",
            teradata_conn_id="td_conn",
        )
        with pytest.raises(ValueError, match="Failed to execute BTEQ script due to invalid file path"):
            op.execute({})

    @mock.patch("airflow.providers.teradata.operators.bteq.is_valid_file", return_value=True)
    @mock.patch(
        "airflow.providers.teradata.operators.bteq.is_valid_encoding",
        side_effect=UnicodeDecodeError("utf8", b"", 0, 1, "error"),
    )
    def test_file_encoding_error(self, mock_encoding, mock_valid_file):
        op = BteqOperator(
            task_id="encoding_fail",
            file_path="/tmp/test.sql",
            bteq_script_encoding="UTF-8",
            teradata_conn_id="td_conn",
        )
        with pytest.raises(
            ValueError,
            match="Failed to execute BTEQ script because the provided file.*encoding differs from the specified BTEQ I/O encoding",
        ):
            op.execute({})

    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.execute_bteq_script")
    @mock.patch("airflow.providers.teradata.operators.bteq.is_valid_file", return_value=True)
    @mock.patch("airflow.providers.teradata.operators.bteq.is_valid_encoding")
    @mock.patch("airflow.providers.teradata.operators.bteq.read_file")
    def test_execute_local_file(
        self,
        mock_read_file,
        mock_valid_encoding,
        mock_valid_file,
        mock_execute_bteq_script,
    ):
        mock_execute_bteq_script.return_value = 0
        sql_content = "SELECT * FROM table_name;"
        mock_read_file.return_value = sql_content

        with tempfile.NamedTemporaryFile("w+", suffix=".sql", delete=False) as tmp_file:
            tmp_file.write(sql_content)
            tmp_file_path = tmp_file.name

        op = BteqOperator(
            task_id="test_bteq_local_file",
            file_path=tmp_file_path,
            teradata_conn_id="teradata_default",
        )

        result = op.execute(context={})

        assert result == 0
        mock_execute_bteq_script.assert_called_once()

    def test_on_kill_calls_hook(self):
        op = BteqOperator(task_id="kill_test", teradata_conn_id="td_conn")
        op._hook = mock.Mock()
        op.on_kill()
        op._hook.on_kill.assert_called_once()

    def test_on_kill_logs_if_no_hook(self):
        op = BteqOperator(task_id="kill_no_hook", teradata_conn_id="td_conn")
        op._hook = None

        with mock.patch.object(op.log, "warning") as mock_log_info:
            op.on_kill()
            mock_log_info.assert_called_once_with("BteqHook was not initialized. Nothing to terminate.")

    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.execute_bteq_script")
    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.get_conn")
    @mock.patch("airflow.providers.teradata.operators.bteq.SSHHook")
    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.__init__", return_value=None)
    def test_remote_execution_with_sql(
        self,
        mock_bteq_hook_init,
        mock_ssh_hook_class,
        mock_get_conn,
        mock_execute_bteq_script,
    ):
        mock_execute_bteq_script.return_value = 0
        mock_ssh_hook_instance = mock.Mock()
        mock_ssh_hook_class.return_value = mock_ssh_hook_instance

        op = BteqOperator(
            task_id="test_remote_sql",
            sql="SELECT * FROM customers;",
            ssh_conn_id="ssh_default",
            teradata_conn_id="teradata_default",
        )

        result = op.execute(context={})

        mock_bteq_hook_init.assert_called_once_with(
            teradata_conn_id="teradata_default", ssh_conn_id="ssh_default"
        )
        mock_execute_bteq_script.assert_called_once()
        assert result == 0

    @mock.patch("airflow.providers.common.compat.sdk.BaseOperator.render_template")
    def test_render_template_in_sql(self, mock_render):
        op = BteqOperator(task_id="render_test", sql="SELECT * FROM {{ params.table }};")
        mock_render.return_value = "SELECT * FROM my_table;"
        rendered_sql = op.render_template("sql", op.sql, context={"params": {"table": "my_table"}})
        assert rendered_sql == "SELECT * FROM my_table;"

    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.execute_bteq_script", return_value=99)
    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.__init__", return_value=None)
    def test_bteq_timeout_with_custom_rc(self, mock_hook_init, mock_exec):
        op = BteqOperator(
            task_id="timeout_case",
            sql="SELECT 1",
            teradata_conn_id="td_conn",
            timeout=30,
            timeout_rc=99,
            bteq_quit_rc=[99],
        )
        result = op.execute({})
        assert result == 99
        mock_exec.assert_called_once()

    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.execute_bteq_script", return_value=42)
    @mock.patch("airflow.providers.teradata.operators.bteq.BteqHook.__init__", return_value=None)
    def test_bteq_return_code_not_in_quit_rc(self, mock_hook_init, mock_exec):
        op = BteqOperator(
            task_id="rc_not_allowed", sql="SELECT 1", teradata_conn_id="td_conn", bteq_quit_rc=[0, 1]
        )
        result = op.execute({})
        assert result == 42  # still returns, but caller can fail on RC if desired


if __name__ == "__main__":
    unittest.main()
