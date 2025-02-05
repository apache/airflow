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
from connexion.exceptions import BadRequestProblem
from flask import jsonify
from starlette import status

from airflow.providers.fab.www.api_connexion.exceptions import (
    BadRequest,
    NotFound,
    PermissionDenied,
    Unauthenticated,
)

if TYPE_CHECKING:
    from flask import Flask

log = logging.getLogger(__name__)


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

    plugins_manager.initialize_flask_plugins()

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

    def handle_bad_request(error):
        response = {"error": "Bad request"}
        return jsonify(response), status.HTTP_400_BAD_REQUEST

    def handle_not_found(error):
        response = {"error": "Not found"}
        return jsonify(response), status.HTTP_404_NOT_FOUND

    def handle_unauthenticated(error):
        response = {"error": "User is not authenticated"}
        return jsonify(response), status.HTTP_401_UNAUTHORIZED

    def handle_denied(error):
        response = {"error": "Access is denied"}
        return jsonify(response), status.HTTP_403_FORBIDDEN

    app.register_error_handler(404, handle_not_found)

    app.register_error_handler(BadRequest, handle_bad_request)
    app.register_error_handler(NotFound, handle_not_found)
    app.register_error_handler(Unauthenticated, handle_unauthenticated)
    app.register_error_handler(PermissionDenied, handle_denied)
