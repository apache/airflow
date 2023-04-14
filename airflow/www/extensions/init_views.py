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
from os import path

from connexion import FlaskApi, ProblemException, Resolver
from connexion.decorators.validation import RequestBodyValidator
from connexion.exceptions import BadRequestProblem
from flask import Flask, request

from airflow.api_connexion.exceptions import common_error_handler
from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.security import permissions
from airflow.utils.yaml import safe_load

log = logging.getLogger(__name__)

# airflow/www/extensions/init_views.py => airflow/
ROOT_APP_DIR = path.abspath(path.join(path.dirname(__file__), path.pardir, path.pardir))


def init_flash_views(app):
    """Init main app view - redirect to FAB"""
    from airflow.www.blueprints import routes

    app.register_blueprint(routes)


def init_appbuilder_views(app):
    """Initialize Web UI views"""
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


def init_plugins(app):
    """Integrate Flask and FAB with plugins"""
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()

    appbuilder = app.appbuilder

    for view in plugins_manager.flask_appbuilder_views:
        name = view.get("name")
        if name:
            log.debug("Adding view %s with menu", name)
            appbuilder.add_view(view["view"], name, category=view["category"])
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
    """Add custom errors handlers"""
    from airflow.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.not_found)


def set_cors_headers_on_response(response):
    """Add response headers"""
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
    """OpenAPI endpoint that lazily resolves the function on first use.

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
    """OpenAPI endpoint resolver that loads lazily on first use.

    This re-implements ``connexion.Resolver.resolve()`` to not eagerly resolve
    the endpoint function (and thus avoid importing it in the process), but only
    return a placeholder that will be actually resolved when the contained
    function is accessed.
    """

    def resolve(self, operation):
        operation_id = self.resolve_operation_id(operation)
        return _LazyResolution(self.resolve_function_from_operation_id, operation_id)


class _CustomErrorRequestBodyValidator(RequestBodyValidator):
    """Custom request body validator that overrides error messages.

    By default, Connextion emits a very generic *None is not of type 'object'*
    error when receiving an empty request body (with the view specifying the
    body as non-nullable). We overrides it to provide a more useful message.
    """

    def validate_schema(self, data, url):
        if not self.is_null_value_valid and data is None:
            raise BadRequestProblem(detail="Request body must not be empty")
        return super().validate_schema(data, url)


def init_api_connexion(app: Flask) -> None:
    """Initialize Stable API"""
    base_path = "/api/v1"

    from airflow.www import views

    @app.errorhandler(404)
    def _handle_api_not_found(ex):
        if request.path.startswith(base_path):
            # 404 errors are never handled on the blueprint level
            # unless raised from a view func so actual 404 errors,
            # i.e. "no route for it" defined, need to be handled
            # here on the application level
            return common_error_handler(ex)
        else:
            return views.not_found(ex)

    @app.errorhandler(405)
    def _handle_method_not_allowed(ex):
        if request.path.startswith(base_path):
            return common_error_handler(ex)
        else:
            return views.method_not_allowed(ex)

    with open(path.join(ROOT_APP_DIR, "api_connexion", "openapi", "v1.yaml")) as f:
        specification = safe_load(f)
    api_bp = FlaskApi(
        specification=specification,
        resolver=_LazyResolver(),
        base_path=base_path,
        options={
            "swagger_ui": conf.getboolean("webserver", "enable_swagger_ui", fallback=True),
            "swagger_path": path.join(ROOT_APP_DIR, "www", "static", "dist", "swagger-ui"),
        },
        strict_validation=True,
        validate_responses=True,
        validator_map={"body": _CustomErrorRequestBodyValidator},
    ).blueprint
    api_bp.after_request(set_cors_headers_on_response)

    app.register_blueprint(api_bp)
    app.register_error_handler(ProblemException, common_error_handler)
    app.extensions["csrf"].exempt(api_bp)


def init_api_internal(app: Flask, standalone_api: bool = False) -> None:
    """Initialize Internal API"""
    if not standalone_api and not conf.getboolean("webserver", "run_internal_api", fallback=False):
        return

    with open(path.join(ROOT_APP_DIR, "api_internal", "openapi", "internal_api_v1.yaml")) as f:
        specification = safe_load(f)
    api_bp = FlaskApi(
        specification=specification,
        base_path="/internal_api/v1",
        options={"swagger_ui": conf.getboolean("webserver", "enable_swagger_ui", fallback=True)},
        strict_validation=True,
        validate_responses=True,
    ).blueprint
    api_bp.after_request(set_cors_headers_on_response)

    app.register_blueprint(api_bp)
    app.after_request_funcs.setdefault(api_bp.name, []).append(set_cors_headers_on_response)
    app.extensions["csrf"].exempt(api_bp)


def init_api_experimental(app):
    """Initialize Experimental API"""
    if not conf.getboolean("api", "enable_experimental_api", fallback=False):
        return
    from airflow.www.api.experimental import endpoints

    warnings.warn(
        "The experimental REST API is deprecated. Please migrate to the stable REST API. "
        "Please note that the experimental API do not have access control. "
        "The authenticated user has full access.",
        RemovedInAirflow3Warning,
    )
    app.register_blueprint(endpoints.api_experimental, url_prefix="/api/experimental")
    app.extensions["csrf"].exempt(endpoints.api_experimental)
