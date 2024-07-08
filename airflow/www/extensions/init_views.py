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
import warnings
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

from connexion import FlaskApi, ProblemException, Resolver
from connexion.decorators.validation import RequestBodyValidator
from connexion.exceptions import BadRequestProblem
from flask import request

from airflow.api_connexion.exceptions import common_error_handler
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.security import permissions
from airflow.utils.yaml import safe_load
from airflow.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from flask import Flask

log = logging.getLogger(__name__)

# airflow/www/extensions/init_views.py => airflow/
ROOT_APP_DIR = Path(__file__).parents[2].resolve()


def init_flash_views(app):
    """Init main app view - redirect to FAB."""
    from airflow.www.blueprints import routes

    app.register_blueprint(routes)


def init_appbuilder_views(app):
    """Initialize Web UI views."""
    from airflow.models import import_all_models

    import_all_models()

    from airflow.www import views

    appbuilder = app.appbuilder

    # Remove the session from scoped_session registry to avoid
    # reusing a session with a disconnected connection
    appbuilder.session.remove()
    appbuilder.add_view_no_menu(views.AutocompleteView())
    appbuilder.add_view_no_menu(views.Airflow())
    appbuilder.add_view(
        views.DagRunModelView,
        permissions.RESOURCE_DAG_RUN,
        category=permissions.RESOURCE_BROWSE_MENU,
        category_icon="fa-globe",
    )
    appbuilder.add_view(
        views.JobModelView, permissions.RESOURCE_JOB, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.LogModelView, permissions.RESOURCE_AUDIT_LOG, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.VariableModelView, permissions.RESOURCE_VARIABLE, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.TaskInstanceModelView,
        permissions.RESOURCE_TASK_INSTANCE,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    appbuilder.add_view(
        views.TaskRescheduleModelView,
        permissions.RESOURCE_TASK_RESCHEDULE,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    appbuilder.add_view(
        views.TriggerModelView,
        permissions.RESOURCE_TRIGGER,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    appbuilder.add_view(
        views.ConfigurationView,
        permissions.RESOURCE_CONFIG,
        category=permissions.RESOURCE_ADMIN_MENU,
        category_icon="fa-user",
    )
    appbuilder.add_view(
        views.ConnectionModelView, permissions.RESOURCE_CONNECTION, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.SlaMissModelView, permissions.RESOURCE_SLA_MISS, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.PluginView, permissions.RESOURCE_PLUGIN, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.ProviderView, permissions.RESOURCE_PROVIDER, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.PoolModelView, permissions.RESOURCE_POOL, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.XComModelView, permissions.RESOURCE_XCOM, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.DagDependenciesView,
        permissions.RESOURCE_DAG_DEPENDENCIES,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    # add_view_no_menu to change item position.
    # I added link in extensions.init_appbuilder_links.init_appbuilder_links
    appbuilder.add_view_no_menu(views.RedocView)
    # Development views
    appbuilder.add_view_no_menu(views.DevView)
    appbuilder.add_view_no_menu(views.DocsView)


def init_plugins(app):
    """Integrate Flask and FAB with plugins."""
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()

    appbuilder = app.appbuilder

    for view in plugins_manager.flask_appbuilder_views:
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

    for menu_link in sorted(
        plugins_manager.flask_appbuilder_menu_links, key=lambda x: (x.get("category", ""), x["name"])
    ):
        log.debug("Adding menu link %s to %s", menu_link["name"], menu_link["href"])
        appbuilder.add_link(**menu_link)

    for blue_print in plugins_manager.flask_blueprints:
        log.debug("Adding blueprint %s:%s", blue_print["name"], blue_print["blueprint"].import_name)
        app.register_blueprint(blue_print["blueprint"])


def init_error_handlers(app: Flask):
    """Add custom errors handlers."""
    from airflow.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.not_found)


def set_cors_headers_on_response(response):
    """Add response headers."""
    allow_headers = conf.get("api", "access_control_allow_headers")
    allow_methods = conf.get("api", "access_control_allow_methods")
    allow_origins = conf.get("api", "access_control_allow_origins")
    if allow_headers:
        response.headers["Access-Control-Allow-Headers"] = allow_headers
    if allow_methods:
        response.headers["Access-Control-Allow-Methods"] = allow_methods
    if allow_origins == "*":
        response.headers["Access-Control-Allow-Origin"] = "*"
    elif allow_origins:
        allowed_origins = allow_origins.split(" ")
        origin = request.environ.get("HTTP_ORIGIN", allowed_origins[0])
        if origin in allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
    return response


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


base_paths: list[str] = []  # contains the list of base paths that have api endpoints


def init_api_error_handlers(app: Flask) -> None:
    """Add error handlers for 404 and 405 errors for existing API paths."""
    from airflow.www import views

    @app.errorhandler(404)
    def _handle_api_not_found(ex):
        if any([request.path.startswith(p) for p in base_paths]):
            # 404 errors are never handled on the blueprint level
            # unless raised from a view func so actual 404 errors,
            # i.e. "no route for it" defined, need to be handled
            # here on the application level
            return common_error_handler(ex)
        else:
            return views.not_found(ex)

    @app.errorhandler(405)
    def _handle_method_not_allowed(ex):
        if any([request.path.startswith(p) for p in base_paths]):
            return common_error_handler(ex)
        else:
            return views.method_not_allowed(ex)

    app.register_error_handler(ProblemException, common_error_handler)


def init_api_connexion(app: Flask) -> None:
    """Initialize Stable API."""
    base_path = "/api/v1"
    base_paths.append(base_path)

    with ROOT_APP_DIR.joinpath("api_connexion", "openapi", "v1.yaml").open() as f:
        specification = safe_load(f)
    api_bp = FlaskApi(
        specification=specification,
        resolver=_LazyResolver(),
        base_path=base_path,
        options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
        strict_validation=True,
        validate_responses=True,
        validator_map={"body": _CustomErrorRequestBodyValidator},
    ).blueprint
    api_bp.after_request(set_cors_headers_on_response)

    app.register_blueprint(api_bp)
    app.extensions["csrf"].exempt(api_bp)


def init_api_internal(app: Flask, standalone_api: bool = False) -> None:
    """Initialize Internal API."""
    if not standalone_api and not conf.getboolean("webserver", "run_internal_api", fallback=False):
        return

    base_paths.append("/internal_api/v1")
    with ROOT_APP_DIR.joinpath("api_internal", "openapi", "internal_api_v1.yaml").open() as f:
        specification = safe_load(f)
    api_bp = FlaskApi(
        specification=specification,
        base_path="/internal_api/v1",
        options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
        strict_validation=True,
        validate_responses=True,
    ).blueprint
    api_bp.after_request(set_cors_headers_on_response)

    app.register_blueprint(api_bp)
    app.after_request_funcs.setdefault(api_bp.name, []).append(set_cors_headers_on_response)
    app.extensions["csrf"].exempt(api_bp)


def init_api_experimental(app):
    """Initialize Experimental API."""
    if not conf.getboolean("api", "enable_experimental_api", fallback=False):
        return
    from airflow.www.api.experimental import endpoints

    warnings.warn(
        "The experimental REST API is deprecated. Please migrate to the stable REST API. "
        "Please note that the experimental API do not have access control. "
        "The authenticated user has full access.",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    base_paths.append("/api/experimental")
    app.register_blueprint(endpoints.api_experimental, url_prefix="/api/experimental")
    app.extensions["csrf"].exempt(endpoints.api_experimental)


def init_api_auth_provider(app):
    """Initialize the API offered by the auth manager."""
    auth_mgr = get_auth_manager()
    blueprint = auth_mgr.get_api_endpoints()
    if blueprint:
        base_paths.append(blueprint.url_prefix)
        app.register_blueprint(blueprint)
        app.extensions["csrf"].exempt(blueprint)
