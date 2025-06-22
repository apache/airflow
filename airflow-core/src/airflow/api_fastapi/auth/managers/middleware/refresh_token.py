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

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_REFRESH_JWT_TOKEN, BaseAuthManager
from airflow.configuration import conf

log = logging.getLogger(__name__)


class RefreshTokenMiddleware(BaseHTTPMiddleware):
    """Middleware that automatically generates and includes auth header for simple auth manager."""

    def __init__(self, app, auth_manager: BaseAuthManager):
        super().__init__(app)
        self.auth_manager = auth_manager

    async def dispatch(self, request: Request, call_next):
        # Extract Authorization header
        auth = request.headers.get("authorization")

        if auth and auth.lower().startswith("bearer "):
            token_str = auth.split(" ", 1)[1]
            user = await self.auth_manager.get_user_from_token(token_str)
            user_refresh_token = getattr(user, "refresh_token", None)
            user_access_token = getattr(user, "access_token", None)
            if user_access_token and user_refresh_token:
                if tokens := self.auth_manager.refresh_token(user_refresh_token):
                    access_token = tokens.get("access_token")
                    refresh_token = tokens.get("refresh_token")
                    # Update the Authorization header with the new token
                    setattr(user, "access_token", access_token)
                    setattr(user, "refresh_token", refresh_token)
                    new_token_with_updated_user = self.auth_manager.generate_jwt(user=user)
                    response = await call_next(request)
                    # Set the new access token in the cookies to update the state of the user in the token
                    secure = bool(conf.get("api", "ssl_cert", fallback=""))
                    response.set_cookie(
                        COOKIE_NAME_REFRESH_JWT_TOKEN, new_token_with_updated_user, secure=secure
                    )
                    log.info("Token refreshed and sent to client")
                    return response
            else:
                log.warning("User does not have a refresh token, cannot refresh.")
        return await call_next(request)
