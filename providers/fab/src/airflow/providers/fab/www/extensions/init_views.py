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

import logging

from airflow.providers.fab.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS

log = logging.getLogger(__name__)


def init_appbuilder_views(app):
    """Initialize Web UI views."""
    from airflow.models import import_all_models
    from airflow.providers.fab.www import views

    import_all_models()

    appbuilder = app.appbuilder

    # Remove the session from scoped_session registry to avoid
    # reusing a session with a disconnected connection
    appbuilder.session.remove()
    appbuilder.add_view_no_menu(views.FabIndexView())


def init_plugins(app):
    """Integrate Flask and FAB with plugins."""
    from airflow import plugins_manager

    if AIRFLOW_V_3_2_PLUS:
        blueprints, appbuilder_views, appbuilder_menu_links = plugins_manager.get_flask_plugins()
    else:
        plugins_manager.initialize_flask_plugins()  # type: ignore
        blueprints = plugins_manager.flask_blueprints  # type: ignore
        appbuilder_views = plugins_manager.flask_appbuilder_views  # type: ignore
        appbuilder_menu_links = plugins_manager.flask_appbuilder_menu_links  # type: ignore

    appbuilder = app.appbuilder

    for view in appbuilder_views:
        name = view.get("name")
        if name:
            filtered_view_kwargs = {k: v for k, v in view.items() if k not in ["view"]}
            log.debug("Adding view %s with menu", name)
            baseview = view.get("view")
            if baseview:
                appbuilder.add_view(baseview, **filtered_view_kwargs)
            else:
                log.error("'view' key is missing for the named view: %s", name)
        else:
            # if 'name' key is missing, intent is to add view without menu
            log.debug("Adding view %s without menu", str(type(view["view"])))
            appbuilder.add_view_no_menu(view["view"])

    # Since Airflow 3.1 flask_appbuilder_menu_links are added to the Airflow 3 UI
    # navbar..
    if not AIRFLOW_V_3_1_PLUS:
        for menu_link in sorted(appbuilder_menu_links, key=lambda x: (x.get("category", ""), x["name"])):
            log.debug("Adding menu link %s to %s", menu_link["name"], menu_link["href"])
            appbuilder.add_link(**menu_link)

    for blue_print in blueprints:
        log.debug("Adding blueprint %s:%s", blue_print["name"], blue_print["blueprint"].import_name)
        app.register_blueprint(blue_print["blueprint"])


def init_error_handlers(app):
    """Add custom errors handlers."""
    from airflow.providers.fab.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.not_found)
