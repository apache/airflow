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

from unittest import mock

import pytest
from flask import Blueprint, Flask
from werkzeug.exceptions import InternalServerError, NotFound

from airflow.providers.fab.www import views
from airflow.providers.fab.www.extensions import init_views as module
from airflow.providers.fab.www.extensions.init_views import init_error_handlers, init_plugins

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

GET_FLASK_PLUGINS = "airflow.plugins_manager.get_flask_plugins"


def _app_with_appbuilder():
    flask_app = Flask(__name__)
    flask_app.appbuilder = mock.MagicMock()
    return flask_app


@pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="reads plugins via get_flask_plugins")
class TestInitPlugins:
    def test_a_named_view_is_added_with_its_menu_options(self):
        flask_app = _app_with_appbuilder()
        baseview = object()
        plugin_view = {"name": "A View", "category": "A Category", "view": baseview}

        with mock.patch(GET_FLASK_PLUGINS, return_value=([], [plugin_view], [])):
            init_plugins(flask_app)

        # ``view`` is the positional argument, so it must not also be forwarded as a kwarg.
        flask_app.appbuilder.add_view.assert_called_once_with(baseview, name="A View", category="A Category")
        flask_app.appbuilder.add_view_no_menu.assert_not_called()

    def test_a_view_without_a_name_is_added_without_a_menu_entry(self):
        flask_app = _app_with_appbuilder()
        baseview = object()

        with mock.patch(GET_FLASK_PLUGINS, return_value=([], [{"view": baseview}], [])):
            init_plugins(flask_app)

        flask_app.appbuilder.add_view_no_menu.assert_called_once_with(baseview)
        flask_app.appbuilder.add_view.assert_not_called()

    def test_a_named_view_with_no_view_object_is_logged_and_skipped(self):
        flask_app = _app_with_appbuilder()

        with (
            mock.patch(GET_FLASK_PLUGINS, return_value=([], [{"name": "Broken"}], [])),
            mock.patch.object(module, "log") as log,
        ):
            init_plugins(flask_app)

        log.error.assert_called_once_with("'view' key is missing for the named view: %s", "Broken")
        flask_app.appbuilder.add_view.assert_not_called()
        flask_app.appbuilder.add_view_no_menu.assert_not_called()

    def test_plugin_blueprints_are_registered_on_the_app(self):
        flask_app = _app_with_appbuilder()
        blueprint = Blueprint("a_plugin", __name__)

        with mock.patch(
            GET_FLASK_PLUGINS,
            return_value=([{"name": "a_plugin", "blueprint": blueprint}], [], []),
        ):
            init_plugins(flask_app)

        assert flask_app.blueprints["a_plugin"] is blueprint


class TestInitErrorHandlers:
    def test_the_fab_error_views_handle_500_and_404(self):
        flask_app = Flask(__name__)

        init_error_handlers(flask_app)

        handlers = flask_app.error_handler_spec[None]
        assert handlers[500][InternalServerError] is views.show_traceback
        assert handlers[404][NotFound] is views.not_found
