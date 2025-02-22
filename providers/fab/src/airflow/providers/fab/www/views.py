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
from urllib.parse import unquote, urljoin, urlsplit

from flask import (
    g,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_appbuilder import IndexView, expose

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.version import version

# Following the release of https://github.com/python/cpython/issues/102153 in Python 3.9.17 on
# June 6, 2023, we are adding extra sanitization of the urls passed to get_safe_url method to make it works
# the same way regardless if the user uses latest Python patchlevel versions or not. This also follows
# a recommended solution by the Python core team.
#
# From: https://github.com/python/cpython/commit/d28bafa2d3e424b6fdcfd7ae7cde8e71d7177369
#
#   We recommend that users of these APIs where the values may be used anywhere
#   with security implications code defensively. Do some verification within your
#   code before trusting a returned component part.  Does that ``scheme`` make
#   sense?  Is that a sensible ``path``?  Is there anything strange about that
#   ``hostname``?  etc.
#
# C0 control and space to be stripped per WHATWG spec.
# == "".join([chr(i) for i in range(0, 0x20 + 1)])
_WHATWG_C0_CONTROL_OR_SPACE = (
    "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c"
    "\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "
)


class FabIndexView(IndexView):
    """
    A simple view that inherits from FAB index view.

    The only goal of this view is to redirect the user to the Airflow 3 UI index page if the user is
    authenticated. It is impossible to redirect the user directly to the Airflow 3 UI index page before
    redirecting them to this page because FAB itself defines the logic redirection and does not allow external
    redirect.
    """

    @expose("/")
    def index(self):
        if g.user is not None and g.user.is_authenticated:
            token = get_auth_manager().get_jwt_token(g.user)
            return redirect(f"/webapp?token={token}", code=302)
        else:
            return super().index()


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


def not_found(error):
    """Show Not Found on screen for any error in the Webserver."""
    return (
        render_template(
            "airflow/error.html",
            hostname="",
            status_code=404,
            error_message="Page cannot be found.",
        ),
        404,
    )


def get_safe_url(url):
    """Given a user-supplied URL, ensure it points to our web server."""
    if not url:
        return url_for("FabIndexView.index")

    # If the url contains semicolon, redirect it to homepage to avoid
    # potential XSS. (Similar to https://github.com/python/cpython/pull/24297/files (bpo-42967))
    if ";" in unquote(url):
        return url_for("FabIndexView.index")

    url = url.lstrip(_WHATWG_C0_CONTROL_OR_SPACE)

    host_url = urlsplit(request.host_url)
    redirect_url = urlsplit(urljoin(request.host_url, url))
    if not (redirect_url.scheme in ("http", "https") and host_url.netloc == redirect_url.netloc):
        return url_for("FabIndexView.index")

    # This will ensure we only redirect to the right scheme/netloc
    return redirect_url.geturl()
