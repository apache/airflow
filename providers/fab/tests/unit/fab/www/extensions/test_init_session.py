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
import time
from contextlib import contextmanager
from datetime import timedelta

import pytest
import time_machine
from flask import Flask, session as builtin_flask_session
from flask_login import LoginManager, current_user, login_user

from airflow.providers.fab.www.extensions.init_session import (
    SESSION_LOGIN_TIME_KEY,
    get_remaining_session_lifetime,
    init_session_max_lifetime,
)

from tests_common.test_utils.config import conf_vars

LOGIN_TIME = datetime.datetime(2026, 9, 9, 12, 0, tzinfo=datetime.timezone.utc)


class FakeUser:
    is_authenticated = True
    is_active = True
    is_anonymous = False

    def get_id(self):
        return "1"


@contextmanager
def build_client(max_lifetime_minutes: int, remember_cookie_name: str | None = None):
    """Yield a test client for an app wired up with ``session_max_lifetime_minutes`` in force."""
    app = Flask(__name__)
    app.secret_key = "test-secret-key"
    if remember_cookie_name:
        app.config["REMEMBER_COOKIE_NAME"] = remember_cookie_name

    login_manager = LoginManager(app)
    login_manager.session_protection = None
    login_manager.user_loader(lambda user_id: FakeUser() if user_id == "1" else None)

    @app.route("/login")
    def login():
        login_user(FakeUser())
        return ""

    @app.route("/whoami")
    def whoami():
        return "authenticated" if current_user.is_authenticated else "anonymous"

    with conf_vars({("fab", "session_max_lifetime_minutes"): str(max_lifetime_minutes)}):
        init_session_max_lifetime(app)
        yield app.test_client()


def get_body(response) -> str:
    return response.get_data(as_text=True)


@pytest.mark.parametrize(
    ("minutes_since_login", "expected"),
    [(30, "authenticated"), (31, "anonymous")],
)
def test_session_expires_at_max_lifetime_despite_activity(minutes_since_login, expected):
    with build_client(30) as client, time_machine.travel(LOGIN_TIME, tick=False) as traveller:
        client.get("/login")
        # Requesting throughout the window must not push the deadline back.
        traveller.shift(timedelta(minutes=minutes_since_login - 1))
        assert get_body(client.get("/whoami")) == "authenticated"
        traveller.shift(timedelta(minutes=1))
        assert get_body(client.get("/whoami")) == expected


def test_session_never_expires_when_max_lifetime_is_disabled():
    with build_client(0) as client, time_machine.travel(LOGIN_TIME, tick=False) as traveller:
        client.get("/login")
        traveller.shift(timedelta(days=30))
        assert get_body(client.get("/whoami")) == "authenticated"


def test_session_predating_the_setting_is_capped_from_its_next_request():
    with build_client(30) as client, time_machine.travel(LOGIN_TIME, tick=False) as traveller:
        with client.session_transaction() as flask_session:
            flask_session["_user_id"] = "1"

        assert get_body(client.get("/whoami")) == "authenticated"
        traveller.shift(timedelta(minutes=31))
        assert get_body(client.get("/whoami")) == "anonymous"


def test_expiring_a_session_clears_the_remember_me_cookie():
    with (
        build_client(30, remember_cookie_name="remember_token") as client,
        time_machine.travel(LOGIN_TIME, tick=False) as traveller,
    ):
        client.get("/login")
        client.set_cookie("remember_token", "some-remember-me-token")
        traveller.shift(timedelta(minutes=31))

        assert get_body(client.get("/whoami")) == "anonymous"
        assert client.get_cookie("remember_token") is None


@pytest.mark.parametrize(
    ("max_lifetime_minutes", "minutes_since_login", "expected"),
    [
        pytest.param(0, 0, None, id="uncapped-when-disabled"),
        pytest.param(30, 0, 1800, id="full-window-at-login"),
        pytest.param(30, 10, 1200, id="shrinks-as-the-session-ages"),
        pytest.param(30, 31, -60, id="negative-once-past-the-deadline"),
    ],
)
def test_get_remaining_session_lifetime(max_lifetime_minutes, minutes_since_login, expected):
    app = Flask(__name__)
    app.secret_key = "test-secret-key"

    with (
        conf_vars({("fab", "session_max_lifetime_minutes"): str(max_lifetime_minutes)}),
        time_machine.travel(LOGIN_TIME, tick=False) as traveller,
        app.test_request_context(),
    ):
        builtin_flask_session[SESSION_LOGIN_TIME_KEY] = time.time()
        traveller.shift(timedelta(minutes=minutes_since_login))

        assert get_remaining_session_lifetime() == expected


def test_remaining_session_lifetime_is_unknown_without_a_login_stamp():
    app = Flask(__name__)
    app.secret_key = "test-secret-key"

    with conf_vars({("fab", "session_max_lifetime_minutes"): "30"}), app.test_request_context():
        assert get_remaining_session_lifetime() is None
