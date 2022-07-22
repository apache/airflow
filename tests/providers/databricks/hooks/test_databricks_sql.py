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
#

import unittest
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.session import provide_session

TASK_ID = 'databricks-sql-operator'
DEFAULT_CONN_ID = 'databricks_default'
HOST = 'xx.cloud.databricks.com'
HOST_WITH_SCHEME = 'https://xx.cloud.databricks.com'
TOKEN = 'token'


class TestDatabricksSqlHookQueryByName(unittest.TestCase):
    """
    Tests for DatabricksHook.
    """

    @provide_session
    def setUp(self, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = None
        conn.password = TOKEN
        conn.extra = None
        session.commit()

        self.hook = DatabricksSqlHook(sql_endpoint_name="Test")

    @mock.patch('airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook.get_conn')
    @mock.patch('airflow.providers.databricks.hooks.databricks_base.requests')
    def test_query(self, mock_requests, mock_conn):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = {
            "endpoints": [
                {
                    "id": "1264e5078741679a",
                    "name": "Test",
                    "odbc_params": {
                        "hostname": "xx.cloud.databricks.com",
                        "path": "/sql/1.0/endpoints/1264e5078741679a",
                    },
                }
            ]
        }
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.get.return_value).status_code = status_code_mock

        test_fields = ['id', 'value']
        test_schema = [(field,) for field in test_fields]

        conn = mock_conn.return_value
        cur = mock.MagicMock(rowcount=0, description=test_schema)
        cur.fetchall.return_value = []
        conn.cursor.return_value = cur

        query = "select * from test.test;"
        schema, results = self.hook.run(sql=query, handler=fetch_all_handler)

        assert schema == test_schema
        assert results == []

        cur.execute.assert_has_calls([mock.call(q) for q in [query.rstrip(';')]])
        cur.close.assert_called()

    def test_no_query(self):
        for empty_statement in ([], '', '\n'):
            with pytest.raises(ValueError) as err:
                self.hook.run(sql=empty_statement)
            assert err.value.args[0] == "List of SQL statements is empty"
