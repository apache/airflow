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

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from flask import Flask


def init_wsgi_middleware(flask_app: Flask) -> None:
    """Handle X-Forwarded-* headers and base_url support."""
    # Apply ProxyFix middleware
    if conf.getboolean("fab", "ENABLE_PROXY_FIX"):
        flask_app.wsgi_app = ProxyFix(  # type: ignore
            flask_app.wsgi_app,
            x_for=conf.getint("fab", "PROXY_FIX_X_FOR", fallback=1),
            x_proto=conf.getint("fab", "PROXY_FIX_X_PROTO", fallback=1),
            x_host=conf.getint("fab", "PROXY_FIX_X_HOST", fallback=1),
            x_port=conf.getint("fab", "PROXY_FIX_X_PORT", fallback=1),
            x_prefix=conf.getint("fab", "PROXY_FIX_X_PREFIX", fallback=1),
        )
