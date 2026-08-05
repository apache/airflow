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
"""Exceptions raised by the common sandbox executor engine."""

from __future__ import annotations


class SandboxError(Exception):
    """Base exception for the common sandbox executor engine."""


class SandboxConfigurationError(SandboxError, ValueError):
    """Raised when portable sandbox executor configuration is invalid."""


class SandboxProtocolError(SandboxError):
    """Raised when a sandbox driver returns an invalid or unknown response."""


class SandboxInvalidHandleError(SandboxProtocolError):
    """Raised when a persisted provider handle is deterministically invalid."""


class SandboxLaunchUnfencedError(SandboxError):
    """A launch failed and the driver could not prove that its workload was stopped."""

    def __init__(self, request_id: str, launch_error: BaseException, fence_error: BaseException) -> None:
        self.request_id = request_id
        self.launch_error = launch_error
        self.fence_error = fence_error
        super().__init__(
            f"sandbox launch for request {request_id} failed and could not be fenced: {fence_error}"
        )
