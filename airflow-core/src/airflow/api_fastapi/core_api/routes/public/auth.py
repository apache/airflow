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

from fastapi import HTTPException, Request, status
from fastapi.responses import RedirectResponse

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import AuthManagerDep, is_safe_url
from airflow.configuration import conf

auth_router = AirflowRouter(tags=["Login"], prefix="/auth")


@auth_router.get(
    "/login",
    responses=create_openapi_http_exception_doc([status.HTTP_307_TEMPORARY_REDIRECT]),
)
def login(request: Request, auth_manager: AuthManagerDep, next: None | str = None) -> RedirectResponse:
    """Redirect to the login URL depending on the AuthManager configured."""
    login_url = auth_manager.get_url_login()

    if next and not is_safe_url(next, request=request):
        raise HTTPException(status_code=400, detail="Invalid or unsafe next URL")

    if next:
        login_url += f"?next={next}"

    return RedirectResponse(login_url)


@auth_router.get(
    "/logout",
    responses=create_openapi_http_exception_doc([status.HTTP_307_TEMPORARY_REDIRECT]),
)
def logout(request: Request, auth_manager: AuthManagerDep, next: None | str = None) -> RedirectResponse:
    """Logout the user."""
    logout_url = auth_manager.get_url_logout()
    if logout_url:
        return RedirectResponse(logout_url)

    secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
    response = RedirectResponse(auth_manager.get_url_login())
    response.delete_cookie(
        key=COOKIE_NAME_JWT_TOKEN,
        secure=secure,
        httponly=True,
    )

    return response
