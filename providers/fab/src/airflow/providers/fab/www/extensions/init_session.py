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

import logging
import time

from flask import session as builtin_flask_session
from flask_login import current_user, logout_user, user_logged_in

from airflow.exceptions import AirflowConfigException
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.www.session import (
    AirflowDatabaseSessionInterface,
    AirflowSecureCookieSessionInterface,
)

log = logging.getLogger(__name__)

SESSION_LOGIN_TIME_KEY = "_login_at"


def init_airflow_session_interface(app, db):
    """Set airflow session interface."""
    config = app.config.copy()
    selected_backend = conf.get("fab", "SESSION_BACKEND")
    # A bit of a misnomer - normally cookies expire whenever the browser is closed
    # or when they hit their expiry datetime, whichever comes first. "Permanent"
    # cookies only expire when they hit their expiry datetime, and can outlive
    # the browser being closed.
    permanent_cookie = config.get("SESSION_PERMANENT", True)

    if selected_backend == "securecookie":
        app.session_interface = AirflowSecureCookieSessionInterface()
        if permanent_cookie:

            def make_session_permanent():
                builtin_flask_session.permanent = True

            app.before_request(make_session_permanent)
    elif selected_backend == "database":
        app.session_interface = AirflowDatabaseSessionInterface(
            app=app,
            client=db,
            permanent=permanent_cookie,
            # Typically these would be configurable with Flask-Session,
            # but we will set them explicitly instead as they don't make
            # sense to have configurable in Airflow's use case
            table="session",
            key_prefix="",
            use_signer=True,
        )
    else:
        raise AirflowConfigException(
            "Unrecognized session backend specified in "
            f"[fab] session_backend: '{selected_backend}'. Please set "
            "this to either 'database' or 'securecookie'."
        )


def get_max_session_lifetime_seconds() -> int:
    """Return ``[fab] session_max_lifetime_minutes`` in seconds, or ``0`` when the cap is disabled."""
    return max(conf.getint("fab", "session_max_lifetime_minutes", fallback=0), 0) * 60


def get_remaining_session_lifetime() -> float | None:
    """
    Return how many seconds are left before the current session hits its maximum lifetime.

    ``None`` means the session is not capped, either because ``[fab]
    session_max_lifetime_minutes`` is disabled or because the session carries no login stamp.
    """
    max_lifetime_seconds = get_max_session_lifetime_seconds()
    if not max_lifetime_seconds:
        return None
    login_time = builtin_flask_session.get(SESSION_LOGIN_TIME_KEY)
    if login_time is None:
        return None
    return login_time + max_lifetime_seconds - time.time()


def init_session_max_lifetime(app):
    """Expire sessions ``[fab] session_max_lifetime_minutes`` after login, regardless of activity."""
    if not get_max_session_lifetime_seconds():
        return

    # ``weak=False``: the receiver is a local function, so a weak subscription could be collected.
    @user_logged_in.connect_via(app, weak=False)
    def stamp_login_time(sender, user, **kwargs):
        # Wall clock rather than ``time.monotonic()``: the stamp is persisted in the session and
        # read back by other API server processes, which share no monotonic clock origin.
        builtin_flask_session[SESSION_LOGIN_TIME_KEY] = time.time()

    @app.before_request
    def expire_session_past_max_lifetime():
        if SESSION_LOGIN_TIME_KEY not in builtin_flask_session:
            # Sessions that predate this setting have no stamp; cap them from now on rather than
            # leaving them exempt forever.
            if current_user.is_authenticated:
                builtin_flask_session[SESSION_LOGIN_TIME_KEY] = time.time()
            return
        remaining = get_remaining_session_lifetime()
        if remaining is not None and remaining < 0:
            log.debug("Session reached [fab] session_max_lifetime_minutes, expiring it.")
            # ``logout_user`` rather than emptying the session: it also invalidates the
            # remember-me cookie and drops the user from the request context, so the request that
            # crossed the deadline is itself unauthenticated.
            logout_user()
            builtin_flask_session.pop(SESSION_LOGIN_TIME_KEY, None)
