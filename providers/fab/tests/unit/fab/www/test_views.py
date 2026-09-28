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

import time

import pytest
from flask import Flask, session as builtin_flask_session

from airflow.providers.fab.www.extensions.init_session import SESSION_LOGIN_TIME_KEY
from airflow.providers.fab.www.views import get_token_expiration_seconds

from tests_common.test_utils.config import conf_vars

JWT_EXPIRATION_TIME = 86400


@pytest.fixture
def app():
    flask_app = Flask(__name__)
    flask_app.secret_key = "test-secret-key"
    return flask_app


@pytest.mark.parametrize(
    ("max_lifetime_minutes", "seconds_since_login", "expected"),
    [
        pytest.param(0, 0, JWT_EXPIRATION_TIME, id="uncapped-when-max-lifetime-disabled"),
        pytest.param(480, 0, 480 * 60, id="capped-to-the-session-deadline"),
        pytest.param(480, 600, 480 * 60 - 600, id="capped-to-what-is-left-of-the-session"),
        pytest.param(2880, 0, JWT_EXPIRATION_TIME, id="jwt-expiration-wins-when-it-is-shorter"),
        pytest.param(480, 480 * 60, 1, id="floor-of-one-second-at-the-deadline"),
        pytest.param(480, 480 * 60 + 60, 1, id="floor-of-one-second-past-the-deadline"),
    ],
)
def test_get_token_expiration_seconds(app, max_lifetime_minutes, seconds_since_login, expected):
    overrides = {
        ("api_auth", "jwt_expiration_time"): str(JWT_EXPIRATION_TIME),
        ("fab", "session_max_lifetime_minutes"): str(max_lifetime_minutes),
    }
    with app.test_request_context(), conf_vars(overrides):
        builtin_flask_session[SESSION_LOGIN_TIME_KEY] = time.time() - seconds_since_login
        assert get_token_expiration_seconds() == pytest.approx(expected, abs=1)


def test_token_expiration_is_uncapped_for_a_session_without_a_login_stamp(app):
    overrides = {
        ("api_auth", "jwt_expiration_time"): str(JWT_EXPIRATION_TIME),
        ("fab", "session_max_lifetime_minutes"): "480",
    }
    with app.test_request_context(), conf_vars(overrides):
        assert get_token_expiration_seconds() == JWT_EXPIRATION_TIME
