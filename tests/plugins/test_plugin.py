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

from fastapi import FastAPI
from flask import Blueprint
from flask_appbuilder import BaseView as AppBuilderBaseView, expose

from airflow.executors.base_executor import BaseExecutor

# Importing base classes that we need to derive
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base import BaseSensorOperator
from airflow.task.priority_strategy import PriorityWeightStrategy
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.timetables.interval import CronDataIntervalTimetable
from tests.listeners import empty_listener
from tests.listeners.class_listener import ClassBasedListener
from tests.test_utils.mock_operators import (
    AirflowLink,
    AirflowLink2,
    CustomBaseIndexOpLink,
    CustomOpLink,
    GithubLink,
    GoogleLink,
)


# Will show up under airflow.hooks.test_plugin.PluginHook
class PluginHook(BaseHook):
    pass


# Will show up under airflow.operators.test_plugin.PluginOperator
class PluginOperator(BaseOperator):
    pass


# Will show up under airflow.sensors.test_plugin.PluginSensorOperator
class PluginSensorOperator(BaseSensorOperator):
    pass


# Will show up under airflow.executors.test_plugin.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass


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


# Extend an existing class to avoid the need to implement the full interface
class CustomCronDataIntervalTimetable(CronDataIntervalTimetable):
    pass


class CustomTestTriggerRule(BaseTIDep):
    pass


class CustomPriorityWeightStrategy(PriorityWeightStrategy):
    def get_weight(self, ti):
        return 1


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    operators = [PluginOperator]
    sensors = [PluginSensorOperator]
    hooks = [PluginHook]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    flask_blueprints = [bp]
    fastapi_apps = [app_with_metadata]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = [appbuilder_mitem, appbuilder_mitem_toplevel]
    global_operator_extra_links = [
        AirflowLink(),
        GithubLink(),
    ]
    operator_extra_links = [GoogleLink(), AirflowLink2(), CustomOpLink(), CustomBaseIndexOpLink(1)]
    timetables = [CustomCronDataIntervalTimetable]
    listeners = [empty_listener, ClassBasedListener()]
    ti_deps = [CustomTestTriggerRule()]
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
