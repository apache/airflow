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
from unittest.mock import MagicMock, Mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
except ImportError:
    pytest.skip("MSSQL not available", allow_module_level=True)

MSSQL_DEFAULT = "mssql_default"


class TestMsSqlOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_get_hook_from_conn(self, mock_get_db_hook):
        """
        :class:`~.MsSqlOperator` should use the hook returned by :meth:`airflow.models.Connection.get_hook`
        if one is returned.

        This behavior is necessary in order to support usage of :class:`~.OdbcHook` with this operator.

        Specifically we verify here that :meth:`~.MsSqlOperator.get_hook` returns the hook returned from a
        call of ``get_hook`` on the object returned from :meth:`~.BaseHook.get_connection`.
        """
        mock_hook = MagicMock()
        mock_get_db_hook.return_value = mock_hook

        op = SQLExecuteQueryOperator(task_id="test", sql="", conn_id=MSSQL_DEFAULT)
        assert op.get_db_hook() == mock_hook

    @mock.patch(
        "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook", autospec=MsSqlHook
    )
    def test_get_hook_default(self, mock_get_db_hook):
        """
        If :meth:`airflow.models.Connection.get_hook` does not return a hook (e.g. because of an invalid
        conn type), then :class:`~.MsSqlHook` should be used.
        """
        mock_get_db_hook.return_value.side_effect = Mock(side_effect=AirflowException())

        op = SQLExecuteQueryOperator(task_id="test", sql="", conn_id=MSSQL_DEFAULT)
        assert op.get_db_hook().__class__.__name__ == "MsSqlHook"
