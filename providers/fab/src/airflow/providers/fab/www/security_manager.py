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

from collections.abc import Callable

from flask import current_app, g
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.www.utils import get_method_from_fab_action_map
from airflow.utils.log.logging_mixin import LoggingMixin

EXISTING_ROLES = {
    "Admin",
    "Viewer",
    "User",
    "Op",
    "Public",
}


class AirflowSecurityManagerV2(LoggingMixin):
    """
    Minimal security manager needed to run a Flask application.

    This one is used to run the Flask application needed to run Airflow 2 plugins unless Fab auth manager
    is configured in the environment. In that case, ``FabAirflowSecurityManagerOverride`` is used.
    """

    def __init__(self, appbuilder) -> None:
        super().__init__()
        self.appbuilder = appbuilder

        # Setup Flask-Limiter
        self.limiter = self.create_limiter()

    @staticmethod
    def before_request():
        """Run hook before request."""
        if hasattr(get_auth_manager(), "get_user"):
            g.user = get_auth_manager().get_user()

    def create_limiter(self) -> Limiter:
        limiter = Limiter(key_func=current_app.config.get("RATELIMIT_KEY_FUNC", get_remote_address))
        limiter.init_app(current_app)
        return limiter

    def has_access(
        self, action_name: str, resource_name: str, user=None, resource_pk: str | None = None
    ) -> bool:
        """
        Verify whether a given user could perform a certain action on the given resource.

        Example actions might include can_read, can_write, can_delete, etc.

        This function is called by FAB when accessing a view. See
        https://github.com/dpgaspar/Flask-AppBuilder/blob/c6fecdc551629e15467fde5d06b4437379d90592/flask_appbuilder/security/decorators.py#L134

        :param action_name: action_name on resource (e.g can_read, can_edit).
        :param resource_name: name of view-menu or resource.
        :param user: user
        :param resource_pk: the resource primary key (e.g. the connection ID)
        :return: Whether user could perform certain action on the resource.
        """
        if not user:
            user = g.user

        is_authorized_method = self._get_auth_manager_is_authorized_method(resource_name)
        return is_authorized_method(action_name, resource_pk, user)

    def add_limit_view(self, baseview):
        if not baseview.limits:
            return

        for limit in baseview.limits:
            self.limiter.limit(
                limit_value=limit.limit_value,
                key_func=limit.key_func,
                per_method=limit.per_method,
                methods=limit.methods,
                error_message=limit.error_message,
                exempt_when=limit.exempt_when,
                override_defaults=limit.override_defaults,
                deduct_when=limit.deduct_when,
                on_breach=limit.on_breach,
                cost=limit.cost,
            )(baseview.blueprint)

    def _get_auth_manager_is_authorized_method(self, fab_resource_name: str) -> Callable:
        # The user is trying to access a page specific to the auth manager
        # (e.g. the user list view in FabAuthManager) or a page defined in a plugin
        return lambda action, resource_pk, user: get_auth_manager().is_authorized_custom_view(
            method=get_method_from_fab_action_map().get(action, action),
            resource_name=fab_resource_name,
            user=user,
        )
