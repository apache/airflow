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
from functools import cached_property
from typing import TYPE_CHECKING

from connexion import Resolver
from connexion.decorators.validation import RequestBodyValidator
from connexion.exceptions import BadRequestProblem, ProblemException
from flask import request

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS
from airflow.providers.fab.www.api_connexion.exceptions import common_error_handler

if TYPE_CHECKING:
    from flask import Flask

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


class _LazyResolution:
    """
    OpenAPI endpoint that lazily resolves the function on first use.

    This is a stand-in replacement for ``connexion.Resolution`` that implements
    its public attributes ``function`` and ``operation_id``, but the function
    is only resolved when it is first accessed.
    """

    def __init__(self, resolve_func, operation_id):
        self._resolve_func = resolve_func
        self.operation_id = operation_id

    @cached_property
    def function(self):
        return self._resolve_func(self.operation_id)


class _LazyResolver(Resolver):
    """
    OpenAPI endpoint resolver that loads lazily on first use.

    This re-implements ``connexion.Resolver.resolve()`` to not eagerly resolve
    the endpoint function (and thus avoid importing it in the process), but only
    return a placeholder that will be actually resolved when the contained
    function is accessed.
    """

    def resolve(self, operation):
        operation_id = self.resolve_operation_id(operation)
        return _LazyResolution(self.resolve_function_from_operation_id, operation_id)


class _CustomErrorRequestBodyValidator(RequestBodyValidator):
    """
    Custom request body validator that overrides error messages.

    By default, Connextion emits a very generic *None is not of type 'object'*
    error when receiving an empty request body (with the view specifying the
    body as non-nullable). We overrides it to provide a more useful message.
    """

    def validate_schema(self, data, url):
        if not self.is_null_value_valid and data is None:
            raise BadRequestProblem(detail="Request body must not be empty")
        return super().validate_schema(data, url)


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


base_paths: list[str] = []  # contains the list of base paths that have api endpoints


def init_api_error_handlers(app: Flask) -> None:
    """Add error handlers for 404 and 405 errors for existing API paths."""
    from airflow.providers.fab.www import views

    @app.errorhandler(404)
    def _handle_api_not_found(ex):
        if any([request.path.startswith(p) for p in base_paths]):
            # 404 errors are never handled on the blueprint level
            # unless raised from a view func so actual 404 errors,
            # i.e. "no route for it" defined, need to be handled
            # here on the application level
            return common_error_handler(ex)
        return views.not_found(ex)

    @app.errorhandler(405)
    def _handle_method_not_allowed(ex):
        if any([request.path.startswith(p) for p in base_paths]):
            return common_error_handler(ex)
        return views.method_not_allowed(ex)

    app.register_error_handler(ProblemException, common_error_handler)


def init_error_handlers(app: Flask):
    """Add custom errors handlers."""
    from airflow.providers.fab.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.not_found)


def init_api_auth_provider(app):
    """Initialize the API offered by the auth manager."""
    auth_mgr = get_auth_manager()
    blueprint = auth_mgr.get_api_endpoints()
    if blueprint:
        base_paths.append(blueprint.url_prefix)
        app.register_blueprint(blueprint)
        app.extensions["csrf"].exempt(blueprint)
