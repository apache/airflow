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
"""Exceptions raised by the Islo provider."""

from __future__ import annotations


class IsloError(RuntimeError):
    """Base exception for Islo provider failures."""


class IsloConfigurationError(IsloError):
    """Raised when executor or connection configuration is invalid."""


class IsloProtocolError(IsloError):
    """Raised when the Islo API returns an invalid response."""


class IsloUnfencedLaunchError(IsloError):
    """A launch became ambiguous and its sandbox could not yet be deleted."""

    def __init__(self, sandbox_name: str, launch_error: BaseException, delete_error: BaseException) -> None:
        super().__init__(f"launch outcome for sandbox {sandbox_name!r} is unknown and fencing failed")
        self.sandbox_name = sandbox_name
        self.launch_error = launch_error
        self.delete_error = delete_error
