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

pytestmark = pytest.mark.db_test

try:
    from airflow.providers.fab.auth_manager.models import User

    class TestUserModelGetId:
        def test_get_id_returns_str_for_int_id(self, session):
            """
            To ensure get_id() always returns a string.
            This is required because return type str is expected.
            """
            user = User()
            user.id = 999
            result = user.get_id()

            assert isinstance(result, str), f"Expected str, got {type(result)}"
            assert result == "999"

        def test_get_id_returns_str_for_str_id(self, session):
            """
            If the user id is a string, get_id() should return it without any modification.
            """
            user = User()
            user.id = "999"
            result = user.get_id()

            assert isinstance(result, str)
            assert result == "999"

except ModuleNotFoundError:
    pass
