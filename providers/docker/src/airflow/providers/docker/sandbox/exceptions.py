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
"""Exceptions for the development-only Docker Sandboxes integration."""

from __future__ import annotations

from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxError,
    SandboxProtocolError,
)


class DockerSandboxError(SandboxError):
    """Base exception for Docker Sandboxes driver failures."""


class DockerSandboxConfigurationError(SandboxConfigurationError, DockerSandboxError):
    """Raised when Docker Sandboxes driver configuration is invalid."""


class DockerSandboxProtocolError(SandboxProtocolError, DockerSandboxError):
    """Raised when the ``sbx`` CLI or scratch protocol returns invalid data."""


class DockerSandboxCommandError(DockerSandboxError):
    """Raised when an ``sbx`` command fails or exceeds a local resource limit."""
