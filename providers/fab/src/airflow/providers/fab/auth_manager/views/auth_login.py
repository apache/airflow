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
"""Login views that honor ``next`` when the user is already authenticated."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import g, request
from flask_appbuilder import expose
from flask_appbuilder.security.decorators import no_cache
from flask_appbuilder.security.views import AuthDBView, AuthLDAPView

from airflow.providers.fab.www.views import get_safe_url, redirect

if TYPE_CHECKING:
    from werkzeug.wrappers import Response


def redirect_if_authenticated() -> Response | None:
    """
    Redirect an already-authenticated user to the requested ``next`` URL.

    The UI sends the user back to the login page when its API token has expired, even though the
    Flask session is still valid. Flask-AppBuilder's login views then redirect to the index page and
    discard ``next``, so the user loses the page they were opening. ``redirect`` sets a fresh API
    token cookie, so the user lands on the requested page authenticated.
    """
    if g.user is not None and g.user.is_authenticated:
        return redirect(get_safe_url(request.args.get("next")))
    return None


class CustomAuthDBView(AuthDBView):
    """Database login view that honors ``next`` for already-authenticated users."""

    @expose("/login/", methods=["GET", "POST"])
    @no_cache
    def login(self):
        if (response := redirect_if_authenticated()) is not None:
            return response
        return super().login()


class CustomAuthLDAPView(AuthLDAPView):
    """LDAP login view that honors ``next`` for already-authenticated users."""

    @expose("/login/", methods=["GET", "POST"])
    @no_cache
    def login(self):
        if (response := redirect_if_authenticated()) is not None:
            return response
        return super().login()
