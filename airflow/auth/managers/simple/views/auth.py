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

from flask import redirect, request, session, url_for
from flask_appbuilder import expose

from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.utils.state import State
from airflow.www.app import csrf
from airflow.www.views import AirflowBaseView

logger = logging.getLogger(__name__)


class SimpleAuthManagerAuthenticationViews(AirflowBaseView):
    """
    Views to authenticate using the simple auth manager.

    :param users: the list of users defined in the config
    :param passwords: dict associating a username to its password
    """

    def __init__(self, users: list, passwords: dict[str, str]):
        super().__init__()
        self.users = users
        self.passwords = passwords

    @expose("/login")
    def login(self):
        """Start login process."""
        state_color_mapping = State.state_color.copy()
        state_color_mapping["no_status"] = state_color_mapping.pop(None)
        standalone_dag_processor = conf.getboolean(
            "scheduler", "standalone_dag_processor"
        )
        return self.render_template(
            "airflow/login.html",
            disable_nav_bar=True,
            login_submit_url=url_for("SimpleAuthManagerAuthenticationViews.login_submit"),
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            state_color_mapping=state_color_mapping,
            standalone_dag_processor=standalone_dag_processor,
        )

    @expose("/logout", methods=["GET", "POST"])
    def logout(self):
        """Start logout process."""
        session.clear()
        return redirect(url_for("SimpleAuthManagerAuthenticationViews.login"))

    @csrf.exempt
    @expose("/login_submit", methods=("GET", "POST"))
    def login_submit(self):
        """Redirect the user to this callback after login attempt."""
        username = request.form.get("username")
        password = request.form.get("password")

        found_users = [
            user
            for user in self.users
            if user["username"] == username
            and self.passwords[user["username"]] == password
        ]

        if not username or not password or len(found_users) == 0:
            return redirect(
                url_for("SimpleAuthManagerAuthenticationViews.login", error=["1"])
            )

        session["user"] = SimpleAuthManagerUser(
            username=username,
            role=found_users[0]["role"],
        )

        return redirect(url_for("Airflow.index"))
