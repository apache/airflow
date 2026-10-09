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

import json
from unittest import mock

from airflow.providers.common.ai.managed_agents.base import ManagedAgentRef
from airflow.providers.databricks.hooks.genie import DatabricksGenieHook
from airflow.providers.databricks.toolsets.genie import DatabricksGenieToolset


def test_toolset_uses_public_managed_agent_toolset_contract_and_returns_structured_ids():
    hook = mock.create_autospec(DatabricksGenieHook, instance=True)
    hook.resolve_agent.return_value = ManagedAgentRef(platform="databricks.genie", name="space-1")
    hook.consult.return_value = {
        "space_id": "space-1",
        "conversation_id": "conversation-1",
        "message_id": "message-1",
        "status": "COMPLETED",
        "answer": "There are 12 open orders.",
    }
    toolset = DatabricksGenieToolset(hook, "space-1", tool_name="ask_genie")

    output = json.loads(toolset.invoke_sync("How many open orders?"))

    assert output["space_id"] == "space-1"
    assert output["conversation_id"] == "conversation-1"
    assert output["message_id"] == "message-1"
    hook.consult.assert_called_once_with("space-1", "How many open orders?", timeout=None)
