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


def init_airflow_session_interface(app):
    """Set airflow session interface"""
    config = app.config.copy()
    selected_backend = conf.get('webserver', 'SESSION_BACKEND')
    # A bit of a misnomer - normally cookies expire whenever the browser is closed
    # or when they hit their expiry datetime, whichever comes first. "Permanent"
    # cookies only expire when they hit their expiry datetime, and can outlive
    # the browser being closed.
    permanent_cookie = config.get('SESSION_PERMANENT', True)

    if selected_backend == 'securecookie':
        app.session_interface = AirflowSecureCookieSessionInterface()
        if permanent_cookie:

            def make_session_permanent():
                builtin_flask_session.permanent = True

            app.before_request(make_session_permanent)
    elif selected_backend == 'database':
        app.session_interface = AirflowDatabaseSessionInterface(
            app=app,
            db=None,
            permanent=permanent_cookie,
            # Typically these would be configurable with Flask-Session,
            # but we will set them explicitly instead as they don't make
            # sense to have configurable in Airflow's use case
            table='session',
            key_prefix='',
            use_signer=True,
        )
    else:
        raise AirflowConfigException(
            "Unrecognized session backend specified in "
            f"web_server_session_backend: '{selected_backend}'. Please set "
            "this to either 'database' or 'securecookie'."
        )
