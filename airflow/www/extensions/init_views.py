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

import logging
from os import path

import connexion
from connexion import ProblemException
from flask import Flask

log = logging.getLogger(__name__)

# airflow/www/extesions/init_views.py => airflow/
ROOT_APP_DIR = path.abspath(path.join(path.dirname(__file__), path.pardir, path.pardir))


def init_flash_views(app):
    """Init main app view - redirect to FAB"""
    from airflow.www.blueprints import routes

    app.register_blueprint(routes)


def init_appbuilder_views(app):
    """Initialize Web UI views"""
    appbuilder = app.appbuilder
    from airflow.www import views

    # Remove the session from scoped_session registry to avoid
    # reusing a session with a disconnected connection
    appbuilder.session.remove()
    appbuilder.add_view_no_menu(views.Airflow())
    appbuilder.add_view_no_menu(views.DagModelView())
    appbuilder.add_view(views.DagRunModelView, "DAG Runs", category="Browse", category_icon="fa-globe")
    appbuilder.add_view(views.JobModelView, "Jobs", category="Browse")
    appbuilder.add_view(views.LogModelView, "Logs", category="Browse")
    appbuilder.add_view(views.SlaMissModelView, "SLA Misses", category="Browse")
    appbuilder.add_view(views.TaskInstanceModelView, "Task Instances", category="Browse")
    appbuilder.add_view(views.TaskRescheduleModelView, "Task Reschedules", category="Browse")
    appbuilder.add_view(views.ConfigurationView, "Configurations", category="Admin", category_icon="fa-user")
    appbuilder.add_view(views.ConnectionModelView, "Connections", category="Admin")
    appbuilder.add_view(views.PoolModelView, "Pools", category="Admin")
    appbuilder.add_view(views.VariableModelView, "Variables", category="Admin")
    appbuilder.add_view(views.XComModelView, "XComs", category="Admin")
    appbuilder.add_view(views.VersionView, 'Version', category='About', category_icon='fa-th')
    # add_view_no_menu to change item position.
    # I added link in extensions.init_appbuilder_links.init_appbuilder_links
    appbuilder.add_view_no_menu(views.RedocView)


def init_plugins(app):
    """Integrate Flask and FAB with plugins"""
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()

    appbuilder = app.appbuilder

    for view in plugins_manager.flask_appbuilder_views:
        log.debug("Adding view %s", view["name"])
        appbuilder.add_view(view["view"], view["name"], category=view["category"])

    for menu_link in sorted(plugins_manager.flask_appbuilder_menu_links, key=lambda x: x["name"]):
        log.debug("Adding menu link %s", menu_link["name"])
        appbuilder.add_link(
            menu_link["name"],
            href=menu_link["href"],
            category=menu_link["category"],
            category_icon=menu_link["category_icon"],
        )

    for blue_print in plugins_manager.flask_blueprints:
        log.debug("Adding blueprint %s:%s", blue_print["name"], blue_print["blueprint"].import_name)
        app.register_blueprint(blue_print["blueprint"])


def init_error_handlers(app: Flask):
    """Add custom errors handlers"""
    from airflow.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.circles)


def init_api_connexion(app: Flask) -> None:
    """Initialize Stable API"""
    spec_dir = path.join(ROOT_APP_DIR, 'api_connexion', 'openapi')
    connexion_app = connexion.App(__name__, specification_dir=spec_dir, skip_error_handlers=True)
    connexion_app.app = app
    api_bp = connexion_app.add_api(
        specification='v1.yaml', base_path='/api/v1', validate_responses=True, strict_validation=True
    ).blueprint
    app.register_error_handler(ProblemException, connexion_app.common_error_handler)
    app.extensions['csrf'].exempt(api_bp)


def init_api_experimental(app):
    """Initialize Experimental API"""
    from airflow.www.api.experimental import endpoints

    app.register_blueprint(endpoints.api_experimental, url_prefix='/api/experimental')
    app.extensions['csrf'].exempt(endpoints.api_experimental)
