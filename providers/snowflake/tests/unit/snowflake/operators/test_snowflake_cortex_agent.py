#
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

from unittest import mock

import pytest

from airflow.providers.snowflake.hooks.snowflake_cortex_agent import (
    CreateMode,
    SnowflakeCortexAgentHook,
)
from airflow.providers.snowflake.operators.snowflake_cortex_agent import (
    SnowflakeCortexAgentCreateOperator,
    SnowflakeCortexAgentDeleteOperator,
    SnowflakeCortexAgentOperator,
    SnowflakeCortexAgentUpdateOperator,
)

TASK_ID = "run_agent"


class TestSnowflakeCortexAgentOperator:
    @pytest.mark.parametrize(
        ("operator_kwargs", "expected_kwargs"),
        [
            pytest.param(
                {},
                {
                    "thread_id": None,
                    "parent_message_id": None,
                    "tool_choice": None,
                    "models": None,
                    "instructions": None,
                    "orchestration": None,
                    "tools": None,
                    "tool_resources": None,
                    "timeout": 600,
                },
                id="defaults",
            ),
            pytest.param(
                {
                    "thread_id": "thread-id",
                    "parent_message_id": "parent-message-id",
                    "tool_choice": {"type": "auto"},
                    "models": {"orchestration": "test-model"},
                    "instructions": {"response": "Answer concisely."},
                    "orchestration": {"budget": {"seconds": 30}},
                    "tools": [{"tool_spec": {"name": "search"}}],
                    "tool_resources": {"search": {"name": "search_service"}},
                    "timeout": 300,
                },
                {
                    "thread_id": "thread-id",
                    "parent_message_id": "parent-message-id",
                    "tool_choice": {"type": "auto"},
                    "models": {"orchestration": "test-model"},
                    "instructions": {"response": "Answer concisely."},
                    "orchestration": {"budget": {"seconds": 30}},
                    "tools": [{"tool_spec": {"name": "search"}}],
                    "tool_resources": {"search": {"name": "search_service"}},
                    "timeout": 300,
                },
                id="all-arguments",
            ),
        ],
    )
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "run_agent",
        autospec=True,
    )
    def test_execute(self, mock_run_agent, operator_kwargs, expected_kwargs):
        response = {"content": [{"type": "text", "text": "Hello"}]}
        mock_run_agent.return_value = response
        messages = [{"role": "user", "content": "Hello"}]

        operator = SnowflakeCortexAgentOperator(
            task_id=TASK_ID,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=messages,
            **operator_kwargs,
        )

        result = operator.execute(context={})

        mock_run_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=messages,
            **expected_kwargs,
        )
        assert result == response


class TestSnowflakeCortexAgentCreateOperator:
    @pytest.mark.parametrize(
        ("operator_kwargs", "expected_kwargs"),
        [
            pytest.param(
                {},
                {
                    "comment": None,
                    "profile": None,
                    "models": None,
                    "instructions": None,
                    "orchestration": None,
                    "tools": None,
                    "tool_resources": None,
                    "create_mode": CreateMode.ERROR_IF_EXISTS,
                    "timeout": 600,
                },
                id="defaults",
            ),
            pytest.param(
                {
                    "comment": "Created by Airflow",
                    "profile": {"display_name": "Test Agent"},
                    "models": {"orchestration": "test-model"},
                    "instructions": {"response": "Answer concisely."},
                    "orchestration": {"budget": {"seconds": 30}},
                    "tools": [{"tool_spec": {"name": "search"}}],
                    "tool_resources": {"search": {"name": "search_service"}},
                    "create_mode": "orReplace",
                    "timeout": 300,
                },
                {
                    "comment": "Created by Airflow",
                    "profile": {"display_name": "Test Agent"},
                    "models": {"orchestration": "test-model"},
                    "instructions": {"response": "Answer concisely."},
                    "orchestration": {"budget": {"seconds": 30}},
                    "tools": [{"tool_spec": {"name": "search"}}],
                    "tool_resources": {"search": {"name": "search_service"}},
                    "create_mode": CreateMode.OR_REPLACE,
                    "timeout": 300,
                },
                id="all-arguments",
            ),
        ],
    )
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "create_agent",
        autospec=True,
    )
    def test_execute(self, mock_create_agent, operator_kwargs, expected_kwargs):
        response = {"status": "created"}
        mock_create_agent.return_value = response

        operator = SnowflakeCortexAgentCreateOperator(
            task_id="create_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **operator_kwargs,
        )

        result = operator.execute(context={})

        mock_create_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **expected_kwargs,
        )
        assert result == response

    def test_invalid_create_mode(self):
        with pytest.raises(ValueError, match="'OR_REPLACE' is not a valid CreateMode"):
            SnowflakeCortexAgentCreateOperator(
                task_id="create_agent",
                database="MY_DATABASE",
                schema="MY_SCHEMA",
                agent_name="my_agent",
                create_mode="OR_REPLACE",
            )


class TestSnowflakeCortexAgentUpdateOperator:
    @pytest.mark.parametrize(
        ("operator_kwargs", "expected_kwargs"),
        [
            pytest.param(
                {},
                {
                    "comment": None,
                    "profile": None,
                    "models": None,
                    "instructions": None,
                    "orchestration": None,
                    "tools": None,
                    "tool_resources": None,
                    "timeout": 600,
                },
                id="defaults",
            ),
            pytest.param(
                {
                    "comment": "Updated by Airflow",
                    "profile": {"display_name": "Updated Agent"},
                    "models": {"orchestration": "updated-model"},
                    "instructions": {"response": "Answer briefly."},
                    "orchestration": {"budget": {"seconds": 60}},
                    "tools": [{"tool_spec": {"name": "analyst"}}],
                    "tool_resources": {"analyst": {"semantic_view": "MY_VIEW"}},
                    "timeout": 300,
                },
                {
                    "comment": "Updated by Airflow",
                    "profile": {"display_name": "Updated Agent"},
                    "models": {"orchestration": "updated-model"},
                    "instructions": {"response": "Answer briefly."},
                    "orchestration": {"budget": {"seconds": 60}},
                    "tools": [{"tool_spec": {"name": "analyst"}}],
                    "tool_resources": {"analyst": {"semantic_view": "MY_VIEW"}},
                    "timeout": 300,
                },
                id="all-arguments",
            ),
        ],
    )
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "update_agent",
        autospec=True,
    )
    def test_execute(self, mock_update_agent, operator_kwargs, expected_kwargs):
        response = {}
        mock_update_agent.return_value = response

        operator = SnowflakeCortexAgentUpdateOperator(
            task_id="update_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **operator_kwargs,
        )

        result = operator.execute(context={})

        mock_update_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **expected_kwargs,
        )
        assert result == response


class TestSnowflakeCortexAgentDeleteOperator:
    @pytest.mark.parametrize(
        ("operator_kwargs", "expected_kwargs"),
        [
            pytest.param(
                {},
                {"if_exists": False, "timeout": 600},
                id="defaults",
            ),
            pytest.param(
                {"if_exists": True, "timeout": 300},
                {"if_exists": True, "timeout": 300},
                id="all-arguments",
            ),
        ],
    )
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "delete_agent",
        autospec=True,
    )
    def test_execute(self, mock_delete_agent, operator_kwargs, expected_kwargs):
        response = {"status": "deleted"}
        mock_delete_agent.return_value = response

        operator = SnowflakeCortexAgentDeleteOperator(
            task_id="delete_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **operator_kwargs,
        )

        result = operator.execute(context={})

        mock_delete_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            **expected_kwargs,
        )
        assert result == response
