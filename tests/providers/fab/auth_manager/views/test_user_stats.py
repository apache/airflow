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

import pytest

from airflow.security import permissions
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.www import client_with_login


@pytest.fixture(scope="module")
def user_user_stats_reader(app):
    return create_user(
        app,
        username="user_user_stats",
        role_name="role_user_stats",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER_STATS_CHART),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_user_stats_reader(app, user_user_stats_reader):
    app.config["WTF_CSRF_ENABLED"] = False
    return client_with_login(
        app,
        username="user_user_stats_reader",
        password="user_user_stats_reader",
    )


@pytest.mark.db_test
class TestUserStats:
    def test_user_stats(self, client_user_stats_reader):
        resp = client_user_stats_reader.get("/userstatschartview/chart", follow_redirects=True)
        assert resp.status_code == 200
