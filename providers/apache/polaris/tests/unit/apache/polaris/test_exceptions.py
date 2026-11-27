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

import pytest

from airflow.providers.apache.polaris.exceptions import (
    PolarisApiError,
    PolarisAuthenticationError,
    PolarisConflictError,
    PolarisConnectionError,
    PolarisError,
    PolarisForbiddenError,
    PolarisInvalidArgumentError,
    PolarisNotFoundError,
    PolarisRequestError,
    is_retryable_status,
)


@pytest.mark.parametrize(
    ("status", "retryable"),
    [
        (400, False),
        (401, False),
        (403, False),
        (404, False),
        (408, True),
        (409, True),
        (429, True),
        (500, True),
        (501, False),
        (503, True),
    ],
)
def test_is_retryable_status(status, retryable):
    assert is_retryable_status(status) is retryable


class TestPolarisApiError:
    def test_message_and_attributes(self):
        error = PolarisForbiddenError(403, "not authorized for op CREATE_CATALOG", "ForbiddenException")
        assert str(error) == "Polaris API returned HTTP 403: not authorized for op CREATE_CATALOG"
        assert (error.status, error.message, error.error_type) == (
            403,
            "not authorized for op CREATE_CATALOG",
            "ForbiddenException",
        )
        assert isinstance(error, PolarisApiError)
        assert isinstance(error, PolarisError)

    @pytest.mark.parametrize(
        ("error", "retryable"),
        [
            (PolarisConflictError(409, "changed", "CommitFailedException"), True),
            (PolarisConflictError(409, "stale", "PolicyVersionMismatchException"), True),
            (PolarisConflictError(409, "exists", "AlreadyExistsException"), False),
            (PolarisConflictError(409, "same type", "PolicyMappingAlreadyExistsException"), False),
            (PolarisNotFoundError(404, "missing"), False),
            (PolarisApiError(503, "busy"), True),
        ],
    )
    def test_retryable_by_status_and_type(self, error, retryable):
        assert error.retryable is retryable

    @pytest.mark.parametrize(
        ("error_type", "already_exists", "stale_version"),
        [
            ("AlreadyExistsException", True, False),
            ("CommitConflictException", False, True),
            ("PolicyVersionMismatchException", False, True),
            ("NamespaceNotEmptyException", False, False),
            (None, False, False),
        ],
    )
    def test_conflict_kinds(self, error_type, already_exists, stale_version):
        error = PolarisConflictError(409, "conflict", error_type)
        assert (error.already_exists, error.stale_version) == (already_exists, stale_version)


class TestRetryable:
    def test_connection_errors_are_not_retryable(self):
        assert PolarisConnectionError("Host is empty").retryable is False

    def test_invalid_arguments_are_value_errors_and_not_retryable(self):
        error = PolarisInvalidArgumentError("Invalid namespace: 'a..b'")
        assert isinstance(error, ValueError)
        assert error.retryable is False

    def test_request_errors_are_retryable(self):
        assert PolarisRequestError("Connection refused").retryable is True

    def test_authentication_errors_default_to_not_retryable(self):
        assert PolarisAuthenticationError("invalid_client").retryable is False
        assert PolarisAuthenticationError("endpoint unreachable", retryable=True).retryable is True
