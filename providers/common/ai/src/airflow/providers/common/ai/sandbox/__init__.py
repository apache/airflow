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
"""Isolated sandbox backends for the SandboxToolset."""

from __future__ import annotations

from airflow.providers.common.ai.sandbox.base import SandboxBackend, SandboxResult

# SbxSandboxBackend only shells out to the `sbx` CLI (stdlib imports), so it is
# always importable; IsloSandboxBackend needs the optional `islo` SDK.
from airflow.providers.common.ai.sandbox.sbx import SbxSandboxBackend

__all__ = ["IsloSandboxBackend", "SandboxBackend", "SandboxResult", "SbxSandboxBackend"]


def __getattr__(name: str):
    if name == "IsloSandboxBackend":
        try:
            from airflow.providers.common.ai.sandbox.islo import IsloSandboxBackend
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)
        return IsloSandboxBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
