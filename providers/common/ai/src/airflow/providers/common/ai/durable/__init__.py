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
"""Durable execution: replay an agent's completed steps when Airflow retries its task."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.common.ai.durable.capability import AirflowDurability

__all__ = ["AirflowDurability"]


def __getattr__(name: str) -> object:
    # Lazy, so the framework-neutral journal can be imported without pydantic-ai's durable API.
    if name == "AirflowDurability":
        from airflow.providers.common.ai.durable.capability import AirflowDurability

        return AirflowDurability
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
