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

from unittest import mock

import httpx
import pytest
from pydantic import ValidationError

from airflowctl.ctl.utils.device_login import (
    DeviceAuthorization,
    find_oauth_error,
    show_device_code,
    wait_for_device_token,
)
from airflowctl.exceptions import AirflowCtlDeviceCodeLoginException

AUTHORIZATION = DeviceAuthorization(
    device_code="device-code",
    user_code="WDJB-MJHT",
    verification_uri="https://idp.example.com/device",
    verification_uri_complete="https://idp.example.com/device?user_code=WDJB-MJHT",
    expires_in=600,
    interval=5,
)


def _status_error(status_code: int, **kwargs) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://server/auth/token/cli")
    response = httpx.Response(status_code, request=request, **kwargs)
    return httpx.HTTPStatusError("error", request=request, response=response)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock():
    fake = FakeClock()
    with (
        mock.patch("airflowctl.ctl.utils.device_login.time.monotonic", side_effect=fake.monotonic),
        mock.patch("airflowctl.ctl.utils.device_login.time.sleep", side_effect=fake.sleep),
    ):
        yield fake


def _make_request_token(*outcomes: str | httpx.HTTPStatusError) -> mock.Mock:
    def request_token() -> str:
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    remaining = list(outcomes)
    return mock.Mock(side_effect=request_token)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(
            _status_error(400, json={"detail": "authorization_pending"}), "authorization_pending", id="detail"
        ),
        pytest.param(
            _status_error(400, json={"detail": {"error": "slow_down"}}), "slow_down", id="detail-object"
        ),
        pytest.param(_status_error(400, json={"error": "access_denied"}), "access_denied", id="rfc-6749"),
        pytest.param(_status_error(400, json={"detail": [{"msg": "invalid"}]}), None, id="validation"),
        pytest.param(_status_error(400, json=["authorization_pending"]), None, id="not-an-object"),
        pytest.param(_status_error(400, text="authorization_pending"), None, id="not-json"),
        pytest.param(_status_error(403, json={"detail": "authorization_pending"}), None, id="not-400"),
    ],
)
def test_find_oauth_error(error, expected):
    assert find_oauth_error(error) == expected


@pytest.mark.parametrize("field", ["user_code", "verification_uri", "verification_uri_complete"])
def test_device_authorization_rejects_control_characters(field):
    with pytest.raises(ValidationError):
        DeviceAuthorization.model_validate({**AUTHORIZATION.model_dump(), field: "WDJB\x1b[2K-MJHT"})


def test_show_device_code(capsys):
    show_device_code(AUTHORIZATION.model_copy(update={"user_code": "[bold]X"}))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "https://idp.example.com/device and enter the code: [bold]X" in captured.err
    assert "https://idp.example.com/device?user_code=WDJB-MJHT" in captured.err


def test_wait_for_device_token(clock):
    request_token = _make_request_token(
        _status_error(400, json={"detail": "authorization_pending"}),
        _status_error(400, json={"detail": "slow_down"}),
        _status_error(400, json={"detail": "authorization_pending"}),
        "TOKEN",
    )

    token = wait_for_device_token(authorization=AUTHORIZATION, request_token=request_token, timeout=300)

    assert token == "TOKEN"
    assert clock.sleeps == [5, 5, 10, 10]


@pytest.mark.parametrize(
    ("error", "message"),
    [
        pytest.param("access_denied", "Login refused.", id="access-denied"),
        pytest.param("expired_token", "The code expired", id="expired-token"),
    ],
)
def test_wait_for_device_token_stops_on_final_error(clock, error, message):
    request_token = _make_request_token(_status_error(400, json={"detail": error}))

    with pytest.raises(AirflowCtlDeviceCodeLoginException, match=message):
        wait_for_device_token(authorization=AUTHORIZATION, request_token=request_token, timeout=300)


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(_status_error(400, json={"detail": "invalid_grant"}), id="other-oauth-error"),
        pytest.param(_status_error(503, json={"detail": "Keycloak is unavailable"}), id="outage"),
    ],
)
def test_wait_for_device_token_raises_other_errors(clock, error):
    request_token = _make_request_token(error)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        wait_for_device_token(authorization=AUTHORIZATION, request_token=request_token, timeout=300)

    assert exc_info.value is error


@pytest.mark.parametrize(
    ("timeout", "expires_in", "expected_polls"),
    [
        pytest.param(12, 600, 2, id="timeout"),
        pytest.param(300, 12, 2, id="code-lifetime"),
        pytest.param(3, 600, 0, id="shorter-than-interval"),
    ],
)
def test_wait_for_device_token_times_out(clock, timeout, expires_in, expected_polls):
    request_token = mock.Mock(side_effect=_status_error(400, json={"detail": "authorization_pending"}))

    with pytest.raises(
        AirflowCtlDeviceCodeLoginException,
        match=f"No login completed within {min(timeout, expires_in)} seconds",
    ):
        wait_for_device_token(
            authorization=AUTHORIZATION.model_copy(update={"expires_in": expires_in}),
            request_token=request_token,
            timeout=timeout,
        )

    assert request_token.call_count == expected_polls
    assert clock.now <= min(timeout, expires_in)
