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

from unittest.mock import MagicMock

from airflow.providers.common.ai.utils.config import DataSourceConfig
from airflow.providers.common.sql.hooks.sql import DbApiHook

DATASOURCE_CONFIG = DataSourceConfig(
    conn_id="sql_default",
    uri="sqlite://relative/path/to/db",
    table_name="test_table",
    schema={"id": "integer", "name": "varchar"},
)
API_KEY = "gpt_api_key"

PROMPTS = ["generate query for distinct dept"]

TEST_MODEL_NAME = "github:openai/gpt-5-mini"


class DBApiHookForTests(DbApiHook):
    conn_name_attr = "sql_default"
    get_conn = MagicMock(name="conn")
    get_connection = MagicMock()
    dialect_name = "test_dialect"
