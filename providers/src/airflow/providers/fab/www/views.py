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

import sys
import traceback

from flask import (
    g,
    redirect,
    render_template,
)
from flask_appbuilder import IndexView, expose

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.version import version


class FabIndexView(IndexView):
    """
    A simple view that inherits from FAB index view.

    The only goal of this view is to redirect the user to the Airflow 3 UI index page if the user is
    authenticated. It is impossible to redirect the user directly to the Airflow 3 UI index page before
    redirecting them to this page because FAB itself defines the logic redirection and does not allow external
    redirect.

    It is impossible to redirect the user before
    """

    @expose("/")
    def index(self):
        if g.user is not None and g.user.is_authenticated:
            token = get_auth_manager().get_jwt_token(g.user)
            return redirect(f"/webapp?token={token}", code=302)
        else:
            super().index(self)


def show_traceback(error):
    """Show Traceback for a given error."""
    is_logged_in = get_auth_manager().is_logged_in()
    return (
        render_template(
            "airflow/traceback.html",
            python_version=sys.version.split(" ")[0] if is_logged_in else "redacted",
            airflow_version=version if is_logged_in else "redacted",
            hostname=(
                get_hostname()
                if conf.getboolean("webserver", "EXPOSE_HOSTNAME") and is_logged_in
                else "redacted"
            ),
            info=(
                traceback.format_exc()
                if conf.getboolean("webserver", "EXPOSE_STACKTRACE") and is_logged_in
                else "Error! Please contact server admin."
            ),
        ),
        500,
    )
