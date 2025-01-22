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

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
from flask import redirect, request, session, url_for
from flask_appbuilder import expose

from airflow.api_fastapi.app import get_auth_manager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.utils.state import State
from airflow.www.app import csrf
from airflow.www.views import AirflowBaseView


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
        return self.render_template(
            "airflow/login.html",
            disable_nav_bar=True,
            login_submit_url=url_for(
                "SimpleAuthManagerAuthenticationViews.login_submit", next=request.args.get("next")
            ),
            auto_refresh_interval=conf.getint("webserver", "auto_refresh_interval"),
            state_color_mapping=state_color_mapping,
        )

    @expose("/logout", methods=["GET", "POST"])
    def logout(self):
        """Start logout process."""
        session.clear()
        return redirect(url_for("SimpleAuthManagerAuthenticationViews.login"))

    @pytest.mark.skip(
        "This test will be deleted soon, meanwhile disabling it because it is flaky. See: https://github.com/apache/airflow/issues/45818"
    )
    @csrf.exempt
    @expose("/login_submit", methods=("GET", "POST"))
    def login_submit(self):
        """Redirect the user to this callback after login attempt."""
        username = request.form.get("username")
        password = request.form.get("password")
        next_url = request.args.get("next")

        found_users = [
            user
            for user in self.users
            if user["username"] == username and self.passwords[user["username"]] == password
        ]

        if not username or not password or len(found_users) == 0:
            return redirect(url_for("SimpleAuthManagerAuthenticationViews.login", error=["1"], next=next_url))

        user = SimpleAuthManagerUser(
            username=username,
            role=found_users[0]["role"],
        )
        # Will be removed once Airflow uses the new UI
        session["user"] = user

        token = get_auth_manager().get_jwt_token(user)

        if next_url:
            return redirect(self._get_redirect_url(next_url, token))
        else:
            return redirect(url_for("Airflow.index", token=token))

    def _get_redirect_url(self, next_url: str, token: str) -> str:
        if self._is_same_domain(next_url, request.url):
            return self._add_query_params(next_url, {"token": token})
        else:
            return url_for("Airflow.index", token=token)

    @staticmethod
    def _is_same_domain(next_url: str, current_url: str) -> bool:
        next_url_infos = urlsplit(next_url)
        current_url_infos = urlsplit(current_url)
        return (
            current_url_infos.netloc.startswith("localhost:")
            or (not next_url_infos.scheme or next_url_infos.scheme == current_url_infos.scheme)
            and (not next_url_infos.netloc or next_url_infos.netloc == current_url_infos.netloc)
        )

    @staticmethod
    def _add_query_params(url: str, params: dict) -> str:
        url_infos = urlsplit(url)
        existing_query = dict(parse_qsl(url_infos.query))
        existing_query.update(params)
        updated_query = urlencode(existing_query, doseq=True)
        return urlunsplit(
            (url_infos.scheme, url_infos.netloc, url_infos.path, updated_query, url_infos.fragment)
        )
