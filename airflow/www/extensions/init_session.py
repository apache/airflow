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

from flask import request, session as flask_session
from flask.sessions import SecureCookieSessionInterface


class AirflowSessionInterface(SecureCookieSessionInterface):
    """
    Airflow cookie session interface.
    Modifications of sessions should be done here because
    the change here is global.
    """

    def save_session(self, *args, **kwargs):
        """Prevent creating session from REST API requests."""
        if request.blueprint == '/api/v1':
            return None
        return super().save_session(*args, **kwargs)


def init_permanent_session(app):
    """Make session permanent to allows us to store data"""

    def make_session_permanent():
        flask_session.permanent = True

    app.before_request(make_session_permanent)


def init_airflow_session_interface(app):
    """Set airflow session interface"""
    app.session_interface = AirflowSessionInterface()
