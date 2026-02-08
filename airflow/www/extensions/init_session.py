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

from flask import session as builtin_flask_session

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.www.session import AirflowDatabaseSessionInterface, AirflowSecureCookieSessionInterface


# Monkey patch flask-session's create_session_model to fix compatibility with Flask-SQLAlchemy 2.5.1
# The issue is that dynamically created Session models don't inherit query_class from db.Model,
# which causes AttributeError when flask-session tries to use .query property.
# This patch ensures query_class is set on the Session model class.
def _patch_flask_session_create_session_model():
    """
    Patch flask-session's create_session_model to ensure query_class compatibility.

    This fixes the issue where flask-session's Session model doesn't have the query_class
    attribute required by Flask-SQLAlchemy's _QueryProperty.
    """
    try:
        from flask_session.sqlalchemy import sqlalchemy as flask_session_module

        _original_create_session_model = flask_session_module.create_session_model
        _session_model = None

        def patched_create_session_model(db, table_name, schema=None, bind_key=None, sequence=None):
            nonlocal _session_model
            if _session_model:
                return _session_model

            # Create new model
            Session = _original_create_session_model(db, table_name, schema, bind_key, sequence)

            # Ensure query_class is set for compatibility with Flask-SQLAlchemy
            # Use db.Query which is always available on the SQLAlchemy instance
            if not hasattr(Session, "query_class"):
                Session.query_class = getattr(db, "Query", None) or getattr(db.Model, "query_class", None)

            _session_model = Session
            return Session

        flask_session_module.create_session_model = patched_create_session_model
    except ImportError:
        # flask-session not installed, no need to patch
        pass


# Apply the patch immediately when this module is imported
_patch_flask_session_create_session_model()

_session_interface = None


def init_airflow_session_interface(app, sqlalchemy_client):
    """Set airflow session interface."""
    config = app.config.copy()
    selected_backend = conf.get("webserver", "SESSION_BACKEND")
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
        global _session_interface
        if _session_interface:
            app.session_interface = _session_interface
            return
        _session_interface = AirflowDatabaseSessionInterface(
            app=app,
            client=sqlalchemy_client,
            permanent=permanent_cookie,
            # Typically these would be configurable with Flask-Session,
            # but we will set them explicitly instead as they don't make
            # sense to have configurable in Airflow's use case
            table="session",
            key_prefix="",
            use_signer=True,
        )
        app.session_interface = _session_interface
    else:
        raise AirflowConfigException(
            "Unrecognized session backend specified in "
            f"web_server_session_backend: '{selected_backend}'. Please set "
            "this to either 'database' or 'securecookie'."
        )
