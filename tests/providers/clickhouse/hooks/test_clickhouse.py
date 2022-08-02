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
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.utils import db

clickhouse_client_mock = Mock(name="clickhouse_client_for_test")


class TestClickHouseHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        db.merge_conn(
            Connection(
                conn_id='clickhouse_default',
                conn_type='clickhouse',
                host='http://127.0.0.1',
                port=9000,
                login='default',
                password='password',
                schema='default',
            )
        )

    @patch(
        "airflow.providers.clickhouse.hooks.clickhouse.ClickHouseClient",
        autospec=True,
        return_value=clickhouse_client_mock,
    )
    def test_get_conn(self, clickhouse_mock):
        clickhouse_hook = ClickHouseHook(database='default')

        assert clickhouse_hook.database == 'default'
        assert clickhouse_hook.client is not None
        assert clickhouse_mock.called
        assert isinstance(clickhouse_hook.client, Mock)

    @patch(
        "airflow.providers.clickhouse.hooks.clickhouse.ClickHouseClient",
        autospec=True,
        return_value=clickhouse_client_mock,
    )
    def test_query(self, clickhouse_mock):
        clickhouse_hook = ClickHouseHook()

        clickhouse_query = "SELECT * FROM gettingstarted.clickstream where customer_id='customer1'"
        clickhouse_hook.query(clickhouse_query)

        assert clickhouse_mock.called
        assert isinstance(clickhouse_hook.client, Mock)
        assert clickhouse_mock.return_value.execute.called

