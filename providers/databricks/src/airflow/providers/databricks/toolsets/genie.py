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
"""Common AI toolset adapter for Databricks Genie."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.common.ai.toolsets.managed_agent import BaseManagedAgentToolset
except ImportError as exc:
    raise AirflowOptionalProviderFeatureException(
        "The Databricks Genie toolset needs the 'common.ai' extra of the Databricks provider: "
        "pip install 'apache-airflow-providers-databricks[common.ai]'."
    ) from exc

if TYPE_CHECKING:
    from airflow.providers.common.ai.managed_agents.base import ManagedAgentRef
    from airflow.providers.databricks.hooks.genie import DatabricksGenieHook


class DatabricksGenieToolset(BaseManagedAgentToolset):
    """Expose a Genie space consultation as one model-callable tool."""

    def __init__(
        self,
        hook: DatabricksGenieHook,
        space_id: str,
        *,
        tool_name: str = "consult_genie",
        description: str | None = None,
        timeout: float | None = None,
    ) -> None:
        super().__init__(
            tool_name=tool_name,
            description=description or "Ask the Databricks Genie space a question about its data.",
            timeout=timeout,
            # Genie POSTs are non-idempotent; don't let a model retry create a duplicate message.
            max_retries=0,
        )
        self._hook = hook
        self._space_id = space_id
        self._hook.resolve_agent(space_id)

    @property
    def agent_ref(self) -> ManagedAgentRef:
        return self._hook.resolve_agent(self._space_id)

    def invoke_sync(self, prompt: str) -> str:
        result = self._hook.consult(self._space_id, prompt, timeout=self.timeout)
        return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


__all__ = ["DatabricksGenieToolset"]
