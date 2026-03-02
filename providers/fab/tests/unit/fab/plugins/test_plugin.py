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

import pytest
from fastapi import FastAPI
from flask import Blueprint
from flask_appbuilder import BaseView as AppBuilderBaseView, expose
from starlette.middleware.base import BaseHTTPMiddleware

# This is the class you derive to create a plugin
from airflow.providers.common.compat.sdk import AirflowPlugin
from airflow.task.priority_strategy import PriorityWeightStrategy
from airflow.timetables.interval import CronDataIntervalTimetable

from tests_common.test_utils.mock_operators import (
    AirflowLink,
    AirflowLink2,
    CustomBaseIndexOpLink,
    CustomOpLink,
    GithubLink,
    GoogleLink,
)
from unit.listeners import empty_listener
from unit.listeners.class_listener import ClassBasedListener

pytestmark = pytest.mark.db_test


# Will show up under airflow.macros.test_plugin.plugin_macro
def plugin_macro():
    pass


# Creating a flask appbuilder BaseView
class PluginTestAppBuilderBaseView(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        return self.render_template("test_plugin/test.html", content="Hello galaxy!")


v_appbuilder_view = PluginTestAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Test View",
    "category": "Test Plugin",
    "view": v_appbuilder_view,
    "label": "Test Label",
}

v_nomenu_appbuilder_package = {"view": v_appbuilder_view}

# Creating flask appbuilder Menu Items
appbuilder_mitem = {
    "name": "Google",
    "href": "https://www.google.com",
    "category": "Search",
}
appbuilder_mitem_toplevel = {
    "name": "apache",
    "href": "https://www.apache.org/",
    "label": "The Apache Software Foundation",
}

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/test_plugin",
)

app = FastAPI()

app_with_metadata = {"app": app, "url_prefix": "/some_prefix", "name": "Name of the App"}


class DummyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        return await call_next(request)


middleware_with_metadata = {
    "middleware": DummyMiddleware,
    "args": [],
    "kwargs": {},
    "name": "Name of the Middleware",
}

external_view_with_metadata = {
    "name": "Test IFrame Airflow Docs",
    "href": "https://airflow.apache.org/",
    "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
    "url_route": "test_iframe_plugin",
    "destination": "nav",
    "category": "browse",
}

react_app_with_metadata = {
    "name": "Test React App",
    "bundle_url": "https://example.com/test-plugin-bundle.js",
    "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
    "url_route": "test_react_app",
    "destination": "nav",
    "category": "browse",
}


# Extend an existing class to avoid the need to implement the full interface
class CustomCronDataIntervalTimetable(CronDataIntervalTimetable):
    pass


class CustomPriorityWeightStrategy(PriorityWeightStrategy):
    def get_weight(self, ti):
        return 1


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    macros = [plugin_macro]
    flask_blueprints = [bp]
    fastapi_apps = [app_with_metadata]
    fastapi_root_middlewares = [middleware_with_metadata]
    external_views = [external_view_with_metadata]
    react_apps = [react_app_with_metadata]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = [appbuilder_mitem, appbuilder_mitem_toplevel]
    global_operator_extra_links = [
        AirflowLink(),
        GithubLink(),
    ]
    operator_extra_links = [GoogleLink(), AirflowLink2(), CustomOpLink(), CustomBaseIndexOpLink(1)]
    timetables = [CustomCronDataIntervalTimetable]
    listeners = [empty_listener, ClassBasedListener()]
    priority_weight_strategies = [CustomPriorityWeightStrategy]


class MockPluginA(AirflowPlugin):
    name = "plugin-a"


class MockPluginB(AirflowPlugin):
    name = "plugin-b"


class MockPluginC(AirflowPlugin):
    name = "plugin-c"


class AirflowTestOnLoadPlugin(AirflowPlugin):
    name = "preload"

    def on_load(self, *args, **kwargs):
        self.name = "postload"
