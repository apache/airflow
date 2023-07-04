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
"""Plugins example"""
from __future__ import annotations

from flask import Blueprint
from flask_appbuilder import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access


class EmptyPluginView(BaseView):
    """Creating a Flask-AppBuilder View"""

    default_view = "index"

    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def index(self):
        """Create default view"""
        return self.render_template("empty_plugin/index.html", name="Empty Plugin")


# Creating a flask blueprint
bp = Blueprint(
    "Empty Plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/empty_plugin",
)


class EmptyPlugin(AirflowPlugin):
    """Defining the plugin class"""

    name = "Empty Plugin"
    flask_blueprints = [bp]
    appbuilder_views = [{"name": "Empty Plugin", "category": "Extra Views", "view": EmptyPluginView()}]
