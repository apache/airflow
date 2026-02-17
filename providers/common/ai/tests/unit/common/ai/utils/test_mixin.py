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

import pytest

from airflow.providers.common.ai.utils.mixins import CommonAIHookMixin
from airflow.sdk import Connection


class CommonAIHookTestMixin(CommonAIHookMixin):
    def __init__(self):
        pass


class TestCommonAIHookMixin:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):

        conn_postgres = Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            password="postgres_password",
            host="postgres_host",
        )
        create_connection_without_db(conn_postgres)

    def test_get_db_api_hook(self):
        """Test to validate it fetches DBAPi based hooks"""
        common_ai_hook_mixin = CommonAIHookTestMixin()
        result = common_ai_hook_mixin.get_db_api_hook("postgres_default")
        assert result.dialect_name == "postgresql"
