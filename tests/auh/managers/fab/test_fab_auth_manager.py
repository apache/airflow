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

import pytest

from airflow.auth.managers.fab.fab_auth_manager import FabAuthManager
from airflow.www.fab_security.sqla.models import User


@pytest.fixture
def auth_manager():
    return FabAuthManager()


class TestFabAuthManager:
    @pytest.mark.parametrize(
        "first_name,last_name,expected",
        [
            ("First", "Last", "First Last"),
            ("First", None, "First"),
            (None, "Last", "Last"),
        ],
    )
    @mock.patch("flask_login.utils._get_user")
    def test_get_user_name(self, mock_current_user, first_name, last_name, expected, auth_manager):
        user = User()
        user.first_name = first_name
        user.last_name = last_name
        mock_current_user.return_value = user

        assert auth_manager.get_user_name() == expected
