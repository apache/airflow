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
    SESSION_TOKEN_GENERATION_KEY,
    get_remaining_session_lifetime,
    init_session_max_lifetime,
    init_session_token_generation,
)

from tests_common.test_utils.config import conf_vars

LOGIN_TIME = datetime.datetime(2026, 9, 9, 12, 0, tzinfo=datetime.UTC)


class FakeUser:
    is_authenticated = True
    is_active = True
    is_anonymous = False

    def __init__(self, token_generation: int = 0):
        self.token_generation = token_generation

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
        login_user(FakeUser(), remember=bool(remember_cookie_name))
        return ""

    @app.route("/whoami")
    def whoami():
        return "authenticated" if current_user.is_authenticated else "anonymous"

    with conf_vars({("fab", "session_max_lifetime_minutes"): str(max_lifetime_minutes)}):
        init_session_max_lifetime(app)
        yield app.test_client()


@contextmanager
def build_token_generation_client(user: FakeUser):
    """Yield a test client for an app that ends sessions created before a password change."""
    app = Flask(__name__)
    app.secret_key = "test-secret-key"

    login_manager = LoginManager(app)
    login_manager.session_protection = None
    login_manager.user_loader(lambda user_id: user if user_id == "1" else None)

    @app.route("/login")
    def login():
        login_user(user)
        return ""

    @app.route("/whoami")
    def whoami():
        return "authenticated" if current_user.is_authenticated else "anonymous"

    init_session_token_generation(app)
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
        login = client.get("/login")
        assert any(h.startswith("remember_token=") for h in login.headers.getlist("Set-Cookie"))
        traveller.shift(timedelta(minutes=31))

        response = client.get("/whoami")

        assert get_body(response) == "anonymous"
        # Read the headers rather than the client cookie jar: the jar accessors differ between the
        # Werkzeug versions we support.
        assert any(h.startswith("remember_token=;") for h in response.headers.getlist("Set-Cookie"))


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


def test_login_records_token_generation_only_on_the_fab_app():
    """
    A login on the FAB app records the user's ``token_generation`` in its session, while a login
    on another Flask app in the same process leaves that app's session alone.
    """
    user = FakeUser(token_generation=3)
    other_app = Flask(__name__)
    other_app.secret_key = "test-secret-key"
    LoginManager(other_app)

    with build_token_generation_client(user) as client:
        client.get("/login")
        with client.session_transaction() as flask_session:
            assert flask_session[SESSION_TOKEN_GENERATION_KEY] == 3
        with other_app.test_request_context():
            assert login_user(user)
            assert SESSION_TOKEN_GENERATION_KEY not in builtin_flask_session


@pytest.mark.parametrize(("password_changes", "expected"), [(0, "authenticated"), (1, "anonymous")])
def test_session_ends_once_its_user_changes_password(password_changes, expected):
    user = FakeUser(token_generation=3)
    with build_token_generation_client(user) as client:
        client.get("/login")
        assert get_body(client.get("/whoami")) == "authenticated"
        user.token_generation += password_changes
        assert get_body(client.get("/whoami")) == expected


@pytest.mark.parametrize(("token_generation", "expected"), [(0, "authenticated"), (1, "anonymous")])
def test_session_predating_the_key_counts_as_the_initial_generation(token_generation, expected):
    with build_token_generation_client(FakeUser(token_generation=token_generation)) as client:
        with client.session_transaction() as flask_session:
            flask_session["_user_id"] = "1"

        assert get_body(client.get("/whoami")) == expected
