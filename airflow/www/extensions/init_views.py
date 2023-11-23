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
from enum import Enum
from functools import cached_property, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import connexion
import starlette.exceptions
from connexion import ProblemException, Resolver
from connexion.options import SwaggerUIOptions
from connexion.problem import problem

from airflow.api_connexion.exceptions import problem_error_handler
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, RemovedInAirflow3Warning
from airflow.security import permissions
from airflow.utils.yaml import safe_load
from airflow.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    import starlette.exceptions
    from connexion.lifecycle import ConnexionRequest, ConnexionResponse
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
    if conf.getboolean("webserver", "enable_swagger_ui", fallback=True):
        appbuilder.add_view_no_menu(views.SwaggerView)
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


# contains map of base paths that have api endpoints


class BaseAPIPaths(Enum):
    """Known Airflow API paths."""

    REST_API = "/api/v1"
    INTERNAL_API = "/internal_api/v1"
    EXPERIMENTAL_API = "/api/experimental"


def get_base_url() -> str:
    """Return base url to prepend to all API routes."""
    webserver_base_url = conf.get_mandatory_value("webserver", "BASE_URL", fallback="")
    if webserver_base_url.endswith("/"):
        raise AirflowConfigException("webserver.base_url conf cannot have a trailing slash.")
    base_url = urlsplit(webserver_base_url)[2]
    if not base_url or base_url == "/":
        base_url = ""
    return base_url


BASE_URL = get_base_url()

auth_mgr_mount_point: str | None = None


@lru_cache(maxsize=1)
def get_enabled_api_paths() -> list[str]:
    enabled_apis = []
    enabled_apis.append(BaseAPIPaths.REST_API.value)
    if conf.getboolean("webserver", "run_internal_api", fallback=False):
        enabled_apis.append(BaseAPIPaths.INTERNAL_API.value)
    if conf.getboolean("api", "enable_experimental_api", fallback=False):
        enabled_apis.append(BaseAPIPaths.EXPERIMENTAL_API.value)
    if auth_mgr_mount_point:
        enabled_apis.append(auth_mgr_mount_point)
    return enabled_apis


def is_current_request_on_api_path() -> bool:
    from flask.globals import request

    return any([request.path.startswith(BASE_URL + p) for p in get_enabled_api_paths()])


def init_api_error_handlers(connexion_app: connexion.FlaskApp) -> None:
    """Add error handlers for 404 and 405 errors for existing API paths."""
    from airflow.www import views

    def _handle_api_not_found(error) -> ConnexionResponse | str:
        if is_current_request_on_api_path():
            # 404 errors are never handled on the blueprint level
            # unless raised from a view func so actual 404 errors,
            # i.e. "no route for it" defined, need to be handled
            # here on the application level
            return connexion_app._http_exception(error)
        return views.not_found(error)

    def _handle_api_method_not_allowed(error) -> ConnexionResponse | str:
        if is_current_request_on_api_path():
            return connexion_app._http_exception(error)
        return views.method_not_allowed(error)

    def _handle_redirect(
        request: ConnexionRequest, ex: starlette.exceptions.HTTPException
    ) -> ConnexionResponse:
        return problem(
            title=connexion.http_facts.HTTP_STATUS_CODES.get(ex.status_code),
            detail=ex.detail,
            headers={"Location": ex.detail},
            status=ex.status_code,
        )

    # in case of 404 and 405 we handle errors at the Flask APP level in order to have access to
    # context and be able to render the error page for the UI
    connexion_app.app.register_error_handler(404, _handle_api_not_found)
    connexion_app.app.register_error_handler(405, _handle_api_method_not_allowed)

    # We should handle redirects at connexion_app level - the requests will be redirected to the target
    # location - so they can return application/problem+json response with the Location header regardless
    # ot the request path - does not matter if it is API or UI request
    connexion_app.add_error_handler(301, _handle_redirect)
    connexion_app.add_error_handler(302, _handle_redirect)
    connexion_app.add_error_handler(307, _handle_redirect)
    connexion_app.add_error_handler(308, _handle_redirect)

    # Everything else we handle at the connexion_app level by default error handler
    connexion_app.add_error_handler(ProblemException, problem_error_handler)


def init_api_connexion(connexion_app: connexion.FlaskApp) -> None:
    """Initialize Stable API."""
    with ROOT_APP_DIR.joinpath("api_connexion", "openapi", "v1.yaml").open() as f:
        specification = safe_load(f)
    swagger_ui_options = SwaggerUIOptions(
        swagger_ui=SWAGGER_ENABLED,
        swagger_ui_template_dir=SWAGGER_BUNDLE,
    )

    connexion_app.add_api(
        specification=specification,
        resolver=_LazyResolver(),
        base_path=BASE_URL + BaseAPIPaths.REST_API.value,
        swagger_ui_options=swagger_ui_options,
        strict_validation=True,
        validate_responses=True,
    )


def init_api_internal(connexion_app: connexion.FlaskApp, standalone_api: bool = False) -> None:
    """Initialize Internal API."""
    if not standalone_api and not conf.getboolean("webserver", "run_internal_api", fallback=False):
        return

    with ROOT_APP_DIR.joinpath("api_internal", "openapi", "internal_api_v1.yaml").open() as f:
        specification = safe_load(f)
    swagger_ui_options = SwaggerUIOptions(
        swagger_ui=SWAGGER_ENABLED,
        swagger_ui_template_dir=SWAGGER_BUNDLE,
    )

    connexion_app.add_api(
        specification=specification,
        base_path=BASE_URL + BaseAPIPaths.INTERNAL_API.value,
        swagger_ui_options=swagger_ui_options,
        strict_validation=True,
        validate_responses=True,
    )


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
    app.register_blueprint(
        endpoints.api_experimental, url_prefix=BASE_URL + BaseAPIPaths.EXPERIMENTAL_API.value
    )
    app.extensions["csrf"].exempt(endpoints.api_experimental)


def init_api_auth_manager(connexion_app: connexion.FlaskApp):
    """Initialize the API offered by the auth manager."""
    global auth_mgr_mount_point
    try:
        auth_mgr_mount_point, specification = get_auth_manager().get_auth_manager_api_specification()
    except NotImplementedError:
        log.warning(
            "Your Auth manager does not have a `get_auth_manager_api_specification` method which"
            "means that it does not provide additional API. You can implement this method and "
            "return None, {} tuple to get rid of this warning."
        )
        return
    if not auth_mgr_mount_point:
        return
    swagger_ui_options = SwaggerUIOptions(
        swagger_ui=conf.getboolean("webserver", "enable_swagger_ui", fallback=True),
        swagger_ui_template_dir=SWAGGER_BUNDLE,
    )
    from airflow.www.extensions.init_views import BASE_URL, _LazyResolver

    connexion_app.add_api(
        specification=specification,
        resolver=_LazyResolver(),
        base_path=BASE_URL + auth_mgr_mount_point,
        swagger_ui_options=swagger_ui_options,
        strict_validation=True,
        validate_responses=True,
    )


def init_cors_middleware(connexion_app: connexion.FlaskApp):
    from starlette.middleware.cors import CORSMiddleware

    connexion_app.add_middleware(
        CORSMiddleware,
        connexion.middleware.MiddlewarePosition.BEFORE_ROUTING,
        allow_origins=conf.get("api", "access_control_allow_origins"),
        allow_credentials=True,
        allow_methods=conf.get("api", "access_control_allow_methods"),
        allow_headers=conf.get("api", "access_control_allow_headers"),
    )
