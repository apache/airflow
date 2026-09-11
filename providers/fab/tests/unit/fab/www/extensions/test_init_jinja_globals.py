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

import contextlib
from unittest import mock

import pytest
from flask import Flask

from airflow.providers.fab.www.extensions import init_jinja_globals as module
from airflow.providers.fab.www.extensions.init_jinja_globals import init_jinja_globals

from tests_common.test_utils.config import conf_vars

MODULE = "airflow.providers.fab.www.extensions.init_jinja_globals"


@contextlib.contextmanager
def _stubbed_environment(hostname="a-host", git_version="git-abc"):
    """Stub out everything ``init_jinja_globals`` reads from outside its own module."""
    with (
        mock.patch(f"{MODULE}.get_auth_manager", return_value="an-auth-manager"),
        mock.patch(f"{MODULE}.get_hostname", return_value=hostname),
        mock.patch(f"{MODULE}.get_airflow_git_version", return_value=git_version),
    ):
        yield


def _jinja_globals(flask_app):
    """Call the context processor ``init_jinja_globals`` registered on the app."""
    return flask_app.template_context_processors[None][-1]()


class TestInitJinjaGlobals:
    @conf_vars({("core", "default_timezone"): "utc"})
    def test_utc_is_normalised_to_upper_case(self):
        flask_app = Flask(__name__)

        with _stubbed_environment():
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["server_timezone"] == "UTC"

    @conf_vars({("core", "default_timezone"): "system"})
    def test_system_resolves_to_the_local_timezone_name(self):
        flask_app = Flask(__name__)

        with (
            _stubbed_environment(),
            mock.patch(
                f"{MODULE}.local_timezone", return_value=mock.Mock(name="Antarctica/Troll")
            ) as local_timezone,
        ):
            local_timezone.return_value.name = "Antarctica/Troll"
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["server_timezone"] == "Antarctica/Troll"

    @conf_vars({("core", "default_timezone"): "system"})
    def test_a_non_callable_local_timezone_is_rejected(self):
        flask_app = Flask(__name__)

        with _stubbed_environment(), mock.patch(f"{MODULE}.local_timezone", "not-callable"):
            with pytest.raises(ValueError, match="`local_timezone` is not callable"):
                init_jinja_globals(flask_app, enable_plugins=False)

    @conf_vars({("core", "default_timezone"): "Europe/Amsterdam"})
    def test_an_explicit_timezone_is_passed_through_unchanged(self):
        flask_app = Flask(__name__)

        with _stubbed_environment():
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["server_timezone"] == "Europe/Amsterdam"

    @conf_vars({("fab", "expose_hostname"): "False"})
    def test_the_hostname_is_redacted_when_not_exposed(self):
        flask_app = Flask(__name__)

        with _stubbed_environment(hostname="a-secret-host"):
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["hostname"] == "redact"

    @conf_vars({("fab", "expose_hostname"): "True"})
    def test_the_real_hostname_is_exposed_when_configured(self):
        flask_app = Flask(__name__)

        with _stubbed_environment(hostname="a-secret-host"):
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["hostname"] == "a-secret-host"

    @pytest.mark.parametrize("enable_plugins", [True, False])
    def test_the_plugin_flag_drives_two_opposite_globals(self, enable_plugins):
        flask_app = Flask(__name__)

        with _stubbed_environment():
            init_jinja_globals(flask_app, enable_plugins=enable_plugins)

            extra_globals = _jinja_globals(flask_app)

        assert extra_globals["show_plugin_message"] is enable_plugins
        assert extra_globals["disable_nav_bar"] is not enable_plugins

    @conf_vars(
        {
            ("fab", "navbar_color"): "#111111",
            ("fab", "navbar_text_color"): "#222222",
            ("fab", "navbar_hover_color"): "#333333",
            ("fab", "navbar_text_hover_color"): "#444444",
        }
    )
    def test_each_navbar_colour_reaches_its_own_global(self):
        flask_app = Flask(__name__)

        with _stubbed_environment():
            init_jinja_globals(flask_app, enable_plugins=False)

            extra_globals = _jinja_globals(flask_app)

        # Four near-identical conf lookups, so distinct values catch a mix-up.
        assert extra_globals["navbar_color"] == "#111111"
        assert extra_globals["navbar_text_color"] == "#222222"
        assert extra_globals["navbar_hover_color"] == "#333333"
        assert extra_globals["navbar_text_hover_color"] == "#444444"

    def test_the_git_version_and_auth_manager_come_from_their_helpers(self):
        flask_app = Flask(__name__)

        with _stubbed_environment(git_version="v3.1.0+abc1234"):
            init_jinja_globals(flask_app, enable_plugins=False)

            extra_globals = _jinja_globals(flask_app)

        assert extra_globals["git_version"] == "v3.1.0+abc1234"
        assert extra_globals["auth_manager"] == "an-auth-manager"

    def test_an_unreadable_airflow_version_is_logged_and_reported_as_none(self):
        flask_app = Flask(__name__)
        failure = RuntimeError("no version here")

        class Unreadable:
            @property
            def __version__(self):
                raise failure

        with (
            _stubbed_environment(),
            mock.patch.object(module, "airflow", Unreadable()),
            mock.patch.object(module, "logger") as logger,
        ):
            init_jinja_globals(flask_app, enable_plugins=False)

            assert _jinja_globals(flask_app)["airflow_version"] is None

        logger.error.assert_called_once_with(failure)
