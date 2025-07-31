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

import datetime
import os
from typing import TYPE_CHECKING

import pytest
import time_machine
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager


API_PATHS = {
    "public": "/api/v2",
    "ui": "/ui",
}


BASE_URL = "http://testserver"


def get_api_path(request):
    """Determine the API path based on the test's subdirectory."""
    test_dir = os.path.dirname(request.path)
    subdirectory_name = test_dir.split("/")[-1]

    return API_PATHS.get(subdirectory_name, "/")


@pytest.fixture
def test_client(request):
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        # set time_very_before to 2014-01-01 00:00:00 and time_very_after to tomorrow
        # to make the JWT token always valid for all test cases with time_machine
        time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)
        with time_machine.travel(time_very_before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
            ).generate(
                auth_manager.serialize_user(SimpleAuthManagerUser(username="test", role="admin")),
            )
        yield TestClient(
            app, headers={"Authorization": f"Bearer {token}"}, base_url=f"{BASE_URL}{get_api_path(request)}"
        )
