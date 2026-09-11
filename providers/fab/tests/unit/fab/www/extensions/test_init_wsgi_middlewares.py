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

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow.providers.fab.www.extensions.init_wsgi_middlewares import init_wsgi_middleware

from tests_common.test_utils.config import conf_vars


class TestInitWsgiMiddleware:
    @conf_vars({("fab", "enable_proxy_fix"): "False"})
    def test_wsgi_app_is_left_alone_when_proxy_fix_is_disabled(self):
        flask_app = Flask(__name__)
        original_wsgi_app = flask_app.wsgi_app

        init_wsgi_middleware(flask_app)

        assert not isinstance(flask_app.wsgi_app, ProxyFix)
        # ``Flask.wsgi_app`` is a method, so a fresh bound object comes back on
        # every access; compare by equality rather than identity.
        assert flask_app.wsgi_app == original_wsgi_app

    @conf_vars(
        {
            ("fab", "enable_proxy_fix"): "True",
            # Deliberately five different values: a mix-up between the five keyword
            # arguments passed to ``ProxyFix`` would otherwise go unnoticed, and
            # Werkzeug's own defaults for x_host, x_port and x_prefix are 0 rather
            # than 1, so a dropped keyword shows up here too.
            ("fab", "proxy_fix_x_for"): "2",
            ("fab", "proxy_fix_x_proto"): "3",
            ("fab", "proxy_fix_x_host"): "4",
            ("fab", "proxy_fix_x_port"): "5",
            ("fab", "proxy_fix_x_prefix"): "6",
        }
    )
    def test_each_proxy_fix_option_reaches_its_own_keyword_argument(self):
        flask_app = Flask(__name__)
        original_wsgi_app = flask_app.wsgi_app

        init_wsgi_middleware(flask_app)

        middleware = flask_app.wsgi_app
        assert isinstance(middleware, ProxyFix)
        assert middleware.app == original_wsgi_app
        assert middleware.x_for == 2
        assert middleware.x_proto == 3
        assert middleware.x_host == 4
        assert middleware.x_port == 5
        assert middleware.x_prefix == 6
