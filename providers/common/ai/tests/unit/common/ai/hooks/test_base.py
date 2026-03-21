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

from typing import Any
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.hooks.base import BaseAIHook


class _ConcreteAIHook(BaseAIHook):
    conn_name_attr = "llm_conn_id"
    default_conn_name = "ai_default"
    conn_type = "generic"
    hook_name = "Test AI Hook"

    def get_conn(self) -> Any:
        return MagicMock()

    def create_agent(
        self,
        *,
        output_type: type = str,
        instructions: str = "",
        toolsets: list | None = None,
        **kwargs: Any,
    ) -> Any:
        return MagicMock()

    def run_agent(self, *, agent: Any, prompt: str) -> Any:
        return "result"

    def test_connection(self) -> tuple[bool, str]:
        return True, "Connection successful"


class TestBaseAIHookInit:
    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError):
            BaseAIHook()  # type: ignore[abstract]

    def test_concrete_subclass_instantiates(self):
        hook = _ConcreteAIHook()
        assert isinstance(hook, BaseAIHook)

    def test_model_id_default_is_none(self):
        hook = _ConcreteAIHook()
        assert hook.model_id is None

    def test_model_id_can_be_overridden_on_subclass(self):
        class _HookWithModel(_ConcreteAIHook):
            model_id = "some-model"

        hook = _HookWithModel()
        assert hook.model_id == "some-model"


class TestBaseAIHookGetConn:
    def test_get_conn_returns_value(self):
        hook = _ConcreteAIHook()
        assert hook.get_conn() is not None


class TestBaseAIHookCreateAgent:
    def test_create_agent_with_instructions(self):
        hook = _ConcreteAIHook()
        agent = hook.create_agent(instructions="Be helpful.")
        assert agent is not None

    def test_create_agent_with_output_type(self):
        hook = _ConcreteAIHook()
        agent = hook.create_agent(output_type=dict, instructions="Return a dict.")
        assert agent is not None

    def test_create_agent_with_toolsets(self):
        hook = _ConcreteAIHook()
        agent = hook.create_agent(instructions="Use tools.", toolsets=[MagicMock()])
        assert agent is not None


class TestBaseAIHookRunAgent:
    def test_run_agent_returns_output(self):
        hook = _ConcreteAIHook()
        agent = hook.create_agent(instructions="Hi.")
        result = hook.run_agent(agent=agent, prompt="Hello")
        assert result == "result"


class TestBaseAIHookTestConnection:
    def test_test_connection_returns_success_tuple(self):
        hook = _ConcreteAIHook()
        ok, msg = hook.test_connection()
        assert ok is True
        assert isinstance(msg, str)


class _MissingGetConn(BaseAIHook):  # type: ignore[abstract]
    def create_agent(self, *, output_type=str, instructions="", toolsets=None, **kw): ...

    def run_agent(self, *, agent, prompt): ...

    def test_connection(self): ...


class _MissingRunAgent(BaseAIHook):  # type: ignore[abstract]
    def get_conn(self): ...

    def create_agent(self, *, output_type=str, instructions="", toolsets=None, **kw): ...

    def test_connection(self): ...


class _MissingCreateAgent(BaseAIHook):  # type: ignore[abstract]
    def get_conn(self): ...

    def run_agent(self, *, agent, prompt): ...

    def test_connection(self): ...


class _MissingTestConnection(BaseAIHook):  # type: ignore[abstract]
    def get_conn(self): ...

    def create_agent(self, *, output_type=str, instructions="", toolsets=None, **kw): ...

    def run_agent(self, *, agent, prompt): ...


class TestBaseAIHookAbstractEnforcement:
    def test_missing_get_conn_raises(self):
        with pytest.raises(TypeError):
            _MissingGetConn()

    def test_missing_run_agent_raises(self):
        with pytest.raises(TypeError):
            _MissingRunAgent()

    def test_missing_create_agent_raises(self):
        with pytest.raises(TypeError):
            _MissingCreateAgent()

    def test_missing_test_connection_raises(self):
        with pytest.raises(TypeError):
            _MissingTestConnection()
