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

import warnings
from contextlib import AbstractContextManager, nullcontext

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import (
    WaiterMaxAttemptsError,
    WaiterNoCredentialsError,
    WaiterTerminalFailure,
)
from airflow.providers.amazon.aws.utils.deferral import build_error_from_trigger_event, should_defer
from airflow.providers.common.compat.sdk import AirflowException


def expect_deferral_warning(*, deferrable: bool, wait_for_completion: bool) -> AbstractContextManager:
    """Expect the ``should_defer`` deprecation warning only for the combination that triggers it."""
    if deferrable and not wait_for_completion:
        return pytest.warns(
            AirflowProviderDeprecationWarning, match="takes precedence over wait_for_completion"
        )
    return nullcontext()


@pytest.mark.parametrize(
    ("deferrable", "wait_for_completion", "expected"),
    [
        pytest.param(False, False, False, id="no_wait"),
        pytest.param(False, True, False, id="wait"),
        pytest.param(True, True, True, id="defer"),
    ],
)
def test_should_defer_without_warning(deferrable, wait_for_completion, expected):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert should_defer(deferrable=deferrable, wait_for_completion=wait_for_completion) is expected


def test_should_defer_warns_when_deferrable_overrides_wait():
    with expect_deferral_warning(deferrable=True, wait_for_completion=False):
        assert should_defer(deferrable=True, wait_for_completion=False) is True


@pytest.mark.parametrize(
    ("event", "expected_class"),
    [
        pytest.param({"error_type": "WaiterTerminalFailure"}, WaiterTerminalFailure, id="terminal-failure"),
        pytest.param({"error_type": "WaiterMaxAttemptsError"}, WaiterMaxAttemptsError, id="max-attempts"),
        pytest.param(
            {"error_type": "WaiterNoCredentialsError"}, WaiterNoCredentialsError, id="no-credentials"
        ),
        pytest.param({"error_type": "AirflowException"}, AirflowException, id="generic-error"),
        pytest.param({"error_type": "ClientError"}, AirflowException, id="class-not-rebuilt"),
        pytest.param({}, AirflowException, id="event-without-error-type"),
    ],
)
def test_build_error_from_trigger_event(event, expected_class):
    error = build_error_from_trigger_event({"status": "error", **event}, "Job failed")

    assert type(error) is expected_class
    assert str(error) == "Job failed"


def test_build_error_from_trigger_event_terminal_failure_has_no_last_response():
    error = build_error_from_trigger_event({"error_type": "WaiterTerminalFailure"}, "Job failed")

    assert error.last_response == {}
