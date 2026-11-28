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
"""Exceptions raised by the Apache Polaris provider."""

from __future__ import annotations


def is_retryable_status(status: int) -> bool:
    """
    Whether a request that failed with this HTTP status may succeed when simply sent again.

    Server errors (5xx, except 501 Not Implemented), timeouts (408), rate limiting (429) and
    conflicts (409, e.g. a concurrent update) are retryable. Other client errors (bad request,
    unauthenticated, forbidden, not found) are not: the request or the access has to change first.
    """
    return status in (408, 409, 429) or (status >= 500 and status != 501)


#: The HTTP 409 type for a create that finds the entity in place.
ALREADY_EXISTS_TYPE = "AlreadyExistsException"
#: HTTP 409 types for an update based on a version that is no longer current; Polaris asks to
#: retry after fetching the latest version.
STALE_VERSION_TYPES = frozenset({"CommitConflictException", "PolicyVersionMismatchException"})
#: Error types that mean a request conflicts with existing state, which a retry does not change.
#: Other 409s (a concurrent commit, a stale policy version) may succeed when retried.
PERMANENT_CONFLICT_TYPES = frozenset(
    {ALREADY_EXISTS_TYPE, "NamespaceNotEmptyException", "PolicyMappingAlreadyExistsException"}
)


class PolarisError(Exception):
    """
    Base class for errors raised by the Apache Polaris provider.

    Not an ``AirflowException`` subclass, matching recent providers: callers catch provider
    errors through this base, and the exceptions carry no Airflow task-state semantics.

    ``retryable`` tells whether retrying the same call could succeed. Operators fail their task
    without retries when it is false; code calling the hooks directly can check it the same way.
    """

    retryable: bool = True


class PolarisConnectionError(PolarisError):
    """A ``polaris`` connection is misconfigured; detected locally before any network call."""

    retryable = False


class PolarisInvalidArgumentError(PolarisError, ValueError):
    """An argument is invalid, e.g. a malformed namespace or policy content that is not JSON."""

    retryable = False


class PolarisRequestError(PolarisError):
    """A request to Polaris did not complete (connection refused, timeout, broken connection)."""


class PolarisAuthenticationError(PolarisError):
    """
    Getting a token failed, or Polaris rejected the token, the realm or the principal's roles.

    :param message: What failed and what to check.
    :param retryable: True when the failure may be transient (the token endpoint was unreachable
        or returned a server error); false when credentials, scope or realm were rejected.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class PolarisApiError(PolarisError):
    """
    A Polaris API call failed for a reason other than authentication.

    :param status: HTTP status code returned by Polaris.
    :param message: Error message, from Polaris's error response when it sent one.
    :param error_type: Polaris error ``type`` (for example ``AlreadyExistsException``), if any.
    """

    def __init__(self, status: int, message: str, error_type: str | None = None) -> None:
        super().__init__(f"Polaris API returned HTTP {status}: {message}")
        self.status = status
        self.message = message
        self.error_type = error_type
        self.retryable = is_retryable_status(status) and error_type not in PERMANENT_CONFLICT_TYPES


class PolarisForbiddenError(PolarisApiError):
    """HTTP 403: authenticated, but the principal's active roles lack a required privilege."""


class PolarisNotFoundError(PolarisApiError):
    """HTTP 404: the catalog, principal, role, namespace, table or policy does not exist."""


class PolarisConflictError(PolarisApiError):
    """
    HTTP 409: the request conflicts with the entity's current state.

    The entity may already exist (:attr:`already_exists`), the update may be based on a version
    that is no longer current (:attr:`stale_version`), or the state may forbid it, for example a
    namespace that is not empty. Only stale versions and other concurrent changes are retryable
    (see :data:`PERMANENT_CONFLICT_TYPES`).
    """

    @property
    def already_exists(self) -> bool:
        """Whether a create found the entity already in place."""
        return self.error_type == ALREADY_EXISTS_TYPE

    @property
    def stale_version(self) -> bool:
        """Whether an update was based on a version that is no longer current."""
        return self.error_type in STALE_VERSION_TYPES
