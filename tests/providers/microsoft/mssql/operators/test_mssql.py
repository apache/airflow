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

import unittest
from unittest import mock
from unittest.mock import MagicMock, Mock

from airflow import PY38, AirflowException

if not PY38:
    from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


class TestMsSqlOperator:
    @unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
    @mock.patch('airflow.hooks.base.BaseHook.get_connection')
    def test_get_hook_from_conn(self, get_connection):
        """
        :class:`~.MsSqlOperator` should use the hook returned by :meth:`airflow.models.Connection.get_hook`
        if one is returned.

        This behavior is necessary in order to support usage of :class:`~.OdbcHook` with this operator.

        Specifically we verify here that :meth:`~.MsSqlOperator.get_hook` returns the hook returned from a
        call of ``get_hook`` on the object returned from :meth:`~.BaseHook.get_connection`.
        """
        mock_hook = MagicMock()
        get_connection.return_value.get_hook.return_value = mock_hook

        op = MsSqlOperator(task_id='test', sql='')
        assert op.get_hook() == mock_hook

    @unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
    @mock.patch('airflow.hooks.base.BaseHook.get_connection')
    def test_get_hook_default(self, get_connection):
        """
        If :meth:`airflow.models.Connection.get_hook` does not return a hook (e.g. because of an invalid
        conn type), then :class:`~.MsSqlHook` should be used.
        """
        get_connection.return_value.get_hook.side_effect = Mock(side_effect=AirflowException())

        op = MsSqlOperator(task_id='test', sql='')
        assert op.get_hook().__class__.__name__ == 'MsSqlHook'
