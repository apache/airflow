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
"""Device code login (RFC 8628) through the CLI login endpoints of the API server."""

from __future__ import annotations

import time
from collections.abc import Callable

import httpx
from pydantic import BaseModel, Field, field_validator
from rich.console import Console
from rich.markup import escape

from airflowctl.exceptions import AirflowCtlDeviceCodeLoginException

DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
_DEFAULT_INTERVAL = 5
_SLOW_DOWN_INCREMENT = 5


class DeviceAuthorization(BaseModel):
    """Response of the ``device_authorization_endpoint``."""

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str | None = None
    expires_in: int = Field(gt=0)
    interval: int = Field(default=_DEFAULT_INTERVAL, gt=0)

    @field_validator("user_code", "verification_uri", "verification_uri_complete")
    @classmethod
    def _check_printable(cls, value: str | None) -> str | None:
        # These are printed to the terminal, so control characters could rewrite what the user sees.
        if value is not None and not value.isprintable():
            raise ValueError("must not contain control characters")
        return value


def find_oauth_error(error: httpx.HTTPStatusError) -> str | None:
    """Return the OAuth error code of a ``400`` from the token endpoint, or ``None``."""
    if error.response.status_code != 400:
        return None
    try:
        body = error.response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    # A FastAPI HTTPException puts it in ``detail``, as a string or as an RFC 6749 error object.
    code = body.get("error") or body.get("detail")
    if isinstance(code, dict):
        code = code.get("error")
    return code if isinstance(code, str) else None


def show_device_code(authorization: DeviceAuthorization) -> None:
    """Tell the user where to approve the login, on stderr because ``auth token`` prints the token on stdout."""
    console = Console(stderr=True, soft_wrap=True)
    console.print(
        f"[blue]On any device, open[/blue] {escape(authorization.verification_uri)} "
        f"[blue]and enter the code:[/blue] [bold]{escape(authorization.user_code)}[/bold]"
    )
    if authorization.verification_uri_complete:
        console.print(f"[blue]Or open:[/blue] {escape(authorization.verification_uri_complete)}")
    console.print("[blue]Waiting for the login to be approved...[/blue]")


def wait_for_device_token(
    *, authorization: DeviceAuthorization, request_token: Callable[[], str], timeout: float
) -> str:
    """
    Poll the token endpoint until the user approves the login and return the CLI token.

    :param authorization: response of the ``device_authorization_endpoint``
    :param request_token: send the ``device_code`` grant to the token endpoint and return the access token
    :param timeout: seconds to wait, capped by the lifetime of the device code
    """
    wait = min(timeout, authorization.expires_in)
    deadline = time.monotonic() + wait
    interval = authorization.interval
    while True:
        if time.monotonic() + interval > deadline:
            raise AirflowCtlDeviceCodeLoginException(f"No login completed within {wait:g} seconds.")
        time.sleep(interval)
        try:
            return request_token()
        except httpx.HTTPStatusError as e:
            error = find_oauth_error(e)
            if error == "authorization_pending":
                continue
            if error == "slow_down":
                interval += _SLOW_DOWN_INCREMENT
                continue
            if error == "access_denied":
                raise AirflowCtlDeviceCodeLoginException("Login refused.") from e
            if error == "expired_token":
                raise AirflowCtlDeviceCodeLoginException(
                    "The code expired before the login was approved."
                ) from e
            raise
