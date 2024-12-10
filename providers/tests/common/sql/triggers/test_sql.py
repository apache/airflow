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

import json
from unittest import mock

import pytest

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger
from airflow.triggers.base import TriggerEvent

from providers.tests.microsoft.azure.base import Base
from tests_common.test_utils.operators.run_deferable import run_trigger
from tests_common.test_utils.version_compat import AIRFLOW_V_2_9_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="Tests for Airflow 2.8.0+ only")


class TestSQLExecuteQueryTrigger(Base):
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_run(self, mock_hook):
        data = [(1, "Alice"), (2, "Bob")]
        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records
        mock_get_records.return_value = data

        trigger = SQLExecuteQueryTrigger(sql="SELECT * FROM users;", conn_id="test_conn_id")
        actual = run_trigger(trigger)

        assert len(actual) == 1
        assert isinstance(actual[0], TriggerEvent)
        assert actual[0].payload["status"] == "success"
        assert actual[0].payload["results"] == json.dumps(data)
