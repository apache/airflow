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
"""
The vendor-neutral contract for managed agents, and the consumers written once over it.

Vendor providers import :mod:`~airflow.providers.common.ai.managed_agents.contract`; it pulls
in nothing from pydantic-ai. The toolset that presents a managed agent to a calling model lives
in :mod:`airflow.providers.common.ai.toolsets.managed_agent`.
"""

from __future__ import annotations

from airflow.providers.common.ai.managed_agents.contract import (
    BaseManagedAgentHook,
    BoundManagedAgent,
    ManagedAgentCapabilities,
    ManagedAgentClient,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
    ManagedAgentUsage,
)
from airflow.providers.common.ai.managed_agents.failover import FailoverManagedAgentClient

__all__ = [
    "BaseManagedAgentHook",
    "BoundManagedAgent",
    "FailoverManagedAgentClient",
    "ManagedAgentCapabilities",
    "ManagedAgentClient",
    "ManagedAgentRef",
    "ManagedAgentRequest",
    "ManagedAgentResponse",
    "ManagedAgentUsage",
]
