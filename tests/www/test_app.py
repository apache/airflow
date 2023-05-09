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

import hashlib
import runpy
import sys
from datetime import timedelta
from unittest import mock

import pytest
from werkzeug.routing import Rule
from werkzeug.test import create_environ
from werkzeug.wrappers import Response

from airflow.exceptions import AirflowConfigException
from airflow.www import app as application
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


class TestApp:
    @classmethod
    def setup_class(cls) -> None:
        from airflow import settings

        settings.configure_orm()

    @conf_vars(
        {
            ("webserver", "enable_proxy_fix"): "True",
            ("webserver", "proxy_fix_x_for"): "1",
            ("webserver", "proxy_fix_x_proto"): "1",
            ("webserver", "proxy_fix_x_host"): "1",
            ("webserver", "proxy_fix_x_port"): "1",
            ("webserver", "proxy_fix_x_prefix"): "1",
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_proxy_fix(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should respect HTTP_X_FORWARDED_FOR
            assert request.remote_addr == "192.168.0.1"
            # Should respect HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX
            assert request.url == "https://valid:445/proxy-prefix/debug"

            return Response("success")

        app.view_functions["debug"] = debug_view

        new_environ = {
            "PATH_INFO": "/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ("webserver", "base_url"): "http://localhost:8080/internal-client",
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_ignore_proxy_headers(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should ignore HTTP_X_FORWARDED_FOR
            assert request.remote_addr == "192.168.0.2"
            # Should ignore HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX
            assert request.url == "http://invalid:9000/internal-client/debug"

            return Response("success")

        app.view_functions["debug"] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ("webserver", "base_url"): "http://localhost:8080/internal-client",
            ("webserver", "enable_proxy_fix"): "True",
            ("webserver", "proxy_fix_x_for"): "1",
            ("webserver", "proxy_fix_x_proto"): "1",
            ("webserver", "proxy_fix_x_host"): "1",
            ("webserver", "proxy_fix_x_port"): "1",
            ("webserver", "proxy_fix_x_prefix"): "1",
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_when_proxy_fix_and_base_url_is_set_up_but_headers_missing(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should use original REMOTE_ADDR
            assert request.remote_addr == "192.168.0.1"
            # Should respect base_url
            assert request.url == "http://invalid:9000/internal-client/debug"

            return Response("success")

        app.view_functions["debug"] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.1",
            "HTTP_HOST": "invalid:9000",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ("webserver", "base_url"): "http://localhost:8080/internal-client",
            ("webserver", "enable_proxy_fix"): "True",
            ("webserver", "proxy_fix_x_for"): "1",
            ("webserver", "proxy_fix_x_proto"): "1",
            ("webserver", "proxy_fix_x_host"): "1",
            ("webserver", "proxy_fix_x_port"): "1",
            ("webserver", "proxy_fix_x_prefix"): "1",
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_and_proxy_when_proxy_fix_and_base_url_is_set_up(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should respect HTTP_X_FORWARDED_FOR
            assert request.remote_addr == "192.168.0.1"
            # Should respect HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX and use base_url
            assert request.url == "https://valid:445/proxy-prefix/internal-client/debug"

            return Response("success")

        app.view_functions["debug"] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ("webserver", "session_lifetime_minutes"): "3600",
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_set_permanent_session_timeout(self):
        app = application.cached_app(testing=True)
        assert app.config["PERMANENT_SESSION_LIFETIME"] == timedelta(minutes=3600)

    @conf_vars({("webserver", "cookie_samesite"): ""})
    @dont_initialize_flask_app_submodules
    def test_correct_default_is_set_for_cookie_samesite(self):
        """An empty 'cookie_samesite' should be corrected to 'Lax' with a deprecation warning."""
        with pytest.deprecated_call():
            app = application.cached_app(testing=True)
        assert app.config["SESSION_COOKIE_SAMESITE"] == "Lax"

    @pytest.mark.parametrize(
        "hash_method, result, exception",
        [
            ("sha512", hashlib.sha512, None),
            ("sha384", hashlib.sha384, None),
            ("sha256", hashlib.sha256, None),
            ("sha224", hashlib.sha224, None),
            ("sha1", hashlib.sha1, None),
            ("md5", hashlib.md5, None),
            (None, hashlib.md5, None),
            ("invalid", None, AirflowConfigException),
        ],
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_caching_hash_method(self, hash_method, result, exception):
        with conf_vars({("webserver", "caching_hash_method"): hash_method}):
            if exception:
                with pytest.raises(expected_exception=exception):
                    app = application.cached_app(testing=True)
            else:
                app = application.cached_app(testing=True)
                assert next(iter(app.extensions["cache"])).cache._hash_method == result


class TestFlaskCli:
    @dont_initialize_flask_app_submodules(skip_all_except=["init_appbuilder"])
    def test_flask_cli_should_display_routes(self, capsys):
        with mock.patch.dict("os.environ", FLASK_APP="airflow.www.app:cached_app"), mock.patch.object(
            sys, "argv", ["flask", "routes"]
        ), pytest.raises(SystemExit):
            from flask import __main__

            # We are not using run_module because of https://github.com/pytest-dev/pytest/issues/9007
            runpy.run_path(__main__.__file__, run_name="main")

        output = capsys.readouterr()
        assert "/login/" in output.out


def test_app_can_json_serialize_k8s_pod():
    # This is mostly testing that we have correctly configured the JSON provider to use. Testing the k8s pos
    # is a side-effect of that.
    k8s = pytest.importorskip("kubernetes.client.models")

    pod = k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))
    app = application.cached_app(testing=True)
    assert app.json.dumps(pod) == '{"spec": {"containers": [{"name": "base"}]}}'
