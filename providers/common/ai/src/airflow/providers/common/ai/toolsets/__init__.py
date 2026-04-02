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
"""Toolsets for exposing Airflow hooks as pydantic-ai agent tools."""

from __future__ import annotations

from airflow.providers.common.ai.toolsets.hook import HookToolset

__all__ = ["HookToolset", "MCPToolset", "SQLToolset"]


def __getattr__(name: str):
    if name == "SQLToolset":
        try:
            from airflow.providers.common.ai.toolsets.sql import SQLToolset
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)
        return SQLToolset
    if name == "MCPToolset":
        try:
            from airflow.providers.common.ai.toolsets.mcp import MCPToolset
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)
        return MCPToolset
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
