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

from typing import TYPE_CHECKING, cast

from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN

if TYPE_CHECKING:
    from fastapi import Request

    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


class FabAuthRolePublicMiddleware(BaseHTTPMiddleware):
    """
    Attach an anonymous user to unauthenticated requests when public access is enabled.

    When ``[fab] auth_role_public`` (or the legacy ``AUTH_ROLE_PUBLIC`` entry in
    ``webserver_config.py``) is set, requests that do not carry any authentication
    token get a pre-populated :class:`AnonymousUser` assigned to
    ``request.state.user``. The FastAPI ``get_user`` dependency picks it up before
    attempting JWT validation, which would otherwise reject the request with 401.
    """

    async def dispatch(self, request: Request, call_next):
        if not self._has_auth_token(request):
            auth_manager = cast("FabAuthManager", get_auth_manager())
            public_user = auth_manager.build_public_user()
            if public_user is not None:
                request.state.user = public_user
        return await call_next(request)

    @staticmethod
    def _has_auth_token(request: Request) -> bool:
        auth_header = request.headers.get("authorization")
        if auth_header:
            return True
        return bool(request.cookies.get(COOKIE_NAME_JWT_TOKEN))
