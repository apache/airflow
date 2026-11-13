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

from typing import TYPE_CHECKING

from werkzeug.middleware.proxy_fix import ProxyFix

from airflow.api_fastapi.app import API_ROOT_PATH, AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from _typeshed.wsgi import StartResponse, WSGIApplication, WSGIEnvironment
    from flask import Flask

_AUTH_MOUNT_PATH_KEY = "airflow.fab.auth_mount_path"


def get_auth_mount_path() -> str:
    """
    Return the path the API server mounts the auth manager app on, relative to its root path.

    ``AUTH_MANAGER_FASTAPI_APP_PREFIX`` already includes the ``[api] base_url`` path, which a
    reverse proxy replaces with ``X-Forwarded-Prefix``; only the trailing mount segment must survive.
    """
    return AUTH_MANAGER_FASTAPI_APP_PREFIX.removeprefix(API_ROOT_PATH.removesuffix("/"))


class _StashAuthMountPath:
    """
    Take the auth manager mount path out of ``SCRIPT_NAME`` before ``ProxyFix`` runs.

    The Flask app is mounted under ``/auth`` of the API server, so WSGI requests arrive with
    ``SCRIPT_NAME`` set to ``<base_url path>/auth``. ``ProxyFix`` assigns ``X-Forwarded-Prefix``
    to ``SCRIPT_NAME`` outright, which drops the ``/auth`` segment and makes every ``url_for``
    result point outside the auth manager app. Only the mount segment is stashed, not the whole
    ``SCRIPT_NAME``, because the forwarded prefix stands in for the ``base_url`` path.
    """

    def __init__(self, app: WSGIApplication, mount_path: str) -> None:
        self.app = app
        self.mount_path = mount_path

    def __call__(self, environ: WSGIEnvironment, start_response: StartResponse):
        script_name = environ.get("SCRIPT_NAME", "")
        if self.mount_path and script_name.endswith(self.mount_path):
            environ["SCRIPT_NAME"] = script_name.removesuffix(self.mount_path)
            environ[_AUTH_MOUNT_PATH_KEY] = self.mount_path
        return self.app(environ, start_response)


class _RestoreAuthMountPath:
    """Append the stashed mount path to whatever prefix ``ProxyFix`` left in ``SCRIPT_NAME``."""

    def __init__(self, app: WSGIApplication) -> None:
        self.app = app

    def __call__(self, environ: WSGIEnvironment, start_response: StartResponse):
        mount_path = environ.pop(_AUTH_MOUNT_PATH_KEY, "")
        if mount_path:
            environ["SCRIPT_NAME"] = environ.get("SCRIPT_NAME", "").rstrip("/") + mount_path
        return self.app(environ, start_response)


def init_wsgi_middleware(flask_app: Flask) -> None:
    """Handle X-Forwarded-* headers and base_url support."""
    # Apply ProxyFix middleware
    if conf.getboolean("fab", "ENABLE_PROXY_FIX"):
        flask_app.wsgi_app = _StashAuthMountPath(  # type: ignore
            ProxyFix(
                _RestoreAuthMountPath(flask_app.wsgi_app),
                x_for=conf.getint("fab", "PROXY_FIX_X_FOR", fallback=1),
                x_proto=conf.getint("fab", "PROXY_FIX_X_PROTO", fallback=1),
                x_host=conf.getint("fab", "PROXY_FIX_X_HOST", fallback=1),
                x_port=conf.getint("fab", "PROXY_FIX_X_PORT", fallback=1),
                x_prefix=conf.getint("fab", "PROXY_FIX_X_PREFIX", fallback=1),
            ),
            mount_path=get_auth_mount_path(),
        )
