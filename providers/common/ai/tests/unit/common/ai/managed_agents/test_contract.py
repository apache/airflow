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

import subprocess
import sys

import pytest

from airflow.providers.common.ai.managed_agents.contract import (
    BaseManagedAgentHook,
    BoundManagedAgent,
    ManagedAgentCapabilities,
    ManagedAgentClient,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
    describe,
)


class RecordingHook(BaseManagedAgentHook):
    """A hook with no vendor behind it, enough to exercise the contract's own logic."""

    agent_platform = "test.platform"

    def __init__(self):
        self.calls: list[tuple[str, ManagedAgentRequest]] = []

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        if not agent:
            raise ValueError("empty agent")
        return ManagedAgentRef(platform=self.agent_platform, name=agent.upper())

    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        return ManagedAgentCapabilities(sessions=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        self.calls.append((agent, request))
        return ManagedAgentResponse(text=f"{agent}: {request.prompt}", raw={"agent": agent})


class TestManagedAgentRequest:
    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"prompt": "a", "messages": []}],
        ids=["neither", "both"],
    )
    def test_needs_exactly_one_of_prompt_or_messages(self, kwargs):
        with pytest.raises(ValueError, match="exactly one of prompt or messages"):
            ManagedAgentRequest(**kwargs)

    def test_a_prompt_renders_as_a_single_user_message(self):
        assert ManagedAgentRequest(prompt="Why?").as_messages() == [
            {"role": "user", "content": [{"type": "text", "text": "Why?"}]}
        ]

    def test_messages_are_copied_not_aliased(self):
        messages = [{"role": "user", "content": "x"}]
        rendered = ManagedAgentRequest(messages=messages).as_messages()
        rendered.append({"role": "user", "content": "y"})
        assert len(messages) == 1

    def test_vendor_options_default_to_a_fresh_dict_per_request(self):
        assert (
            ManagedAgentRequest(prompt="a").vendor_options
            is not ManagedAgentRequest(prompt="b").vendor_options
        )


class TestBaseManagedAgentHook:
    def test_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            BaseManagedAgentHook()  # type: ignore[abstract]

    def test_has_no_init_so_it_can_be_mixed_in_beside_a_vendor_base(self):
        # A vendor hook's own __init__ must win. The contract adds behaviour, not state.
        assert "__init__" not in vars(BaseManagedAgentHook)

    def test_agent_returns_a_bound_pair_that_satisfies_the_client_protocol(self):
        bound = RecordingHook().agent("analyst")
        assert isinstance(bound, BoundManagedAgent)
        assert isinstance(bound, ManagedAgentClient)

    def test_bound_agent_forwards_and_resolves_lazily(self):
        hook = RecordingHook()
        bound = hook.agent("analyst")
        assert bound.ref == ManagedAgentRef(platform="test.platform", name="ANALYST")
        assert bound.capabilities.sessions is True
        response = bound.invoke(ManagedAgentRequest(prompt="q"))
        assert response.text == "analyst: q"
        assert hook.calls == [("analyst", ManagedAgentRequest(prompt="q"))]

    def test_a_bad_agent_fails_at_resolution_not_construction(self):
        bound = RecordingHook().agent("")
        with pytest.raises(ValueError, match="empty agent"):
            bound.ref

    def test_a_session_is_refused_for_a_hook_without_sessions_before_the_hook_is_called(self):
        class Stateless(RecordingHook):
            def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
                return ManagedAgentCapabilities()

        hook = Stateless()
        with pytest.raises(ValueError, match="does not keep conversation state"):
            hook.agent("analyst").invoke(ManagedAgentRequest(prompt="q", session_id="t:1"))
        assert hook.calls == []

    def test_a_session_reaches_a_hook_that_supports_them(self):
        hook = RecordingHook()  # declares sessions=True
        hook.agent("analyst").invoke(ManagedAgentRequest(prompt="q", session_id="t:1"))
        assert hook.calls[0][1].session_id == "t:1"


class TestDescribe:
    def test_renders_name_and_platform(self):
        assert describe(RecordingHook().agent("analyst")) == "ANALYST on test.platform"

    def test_never_raises(self):
        assert describe(RecordingHook().agent("")) == "<unresolved: empty agent>"


def test_contract_module_imports_without_pydantic_ai():
    """
    Vendor providers import the contract behind a guarded import. That import must stay cheap:
    a provider that floors Airflow 2 pays for it on every hook import once the extra is installed.
    """
    code = (
        "import sys\n"
        "import airflow.providers.common.ai.managed_agents.contract\n"
        "import airflow.providers.common.ai.exceptions\n"
        "loaded = sorted(m for m in sys.modules if m == 'pydantic_ai' or m.startswith('pydantic_ai.'))\n"
        "print('PYDANTIC_AI_MODULES=' + repr(loaded))\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    # Airflow's logging may write to stdout on import; only the marker line is the assertion.
    marker = [line for line in result.stdout.splitlines() if line.startswith("PYDANTIC_AI_MODULES=")]
    assert marker == ["PYDANTIC_AI_MODULES=[]"], result.stdout
