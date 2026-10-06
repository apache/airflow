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
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "run_agent",
        autospec=True,
    )
    def test_execute(self, mock_run_agent):
        """Test that the operator delegates execution to the hook."""
        response = {"content": [{"type": "text", "text": "Hello"}]}
        mock_run_agent.return_value = response

        messages = [
            {
                "role": "user",
                "content": "Hello",
            }
        ]

        operator = SnowflakeCortexAgentOperator(
            task_id=TASK_ID,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=messages,
        )

        result = operator.execute(context={})

        mock_run_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=messages,
            thread_id=None,
            parent_message_id=None,
            tool_choice=None,
            models=None,
            instructions=None,
            orchestration=None,
            tools=None,
            tool_resources=None,
            timeout=600,
        )

        assert result == response


class TestSnowflakeCortexAgentCreateOperator:
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "create_agent",
        autospec=True,
    )
    def test_execute(self, mock_create_agent):
        response = {"status": "created"}
        mock_create_agent.return_value = response

        operator = SnowflakeCortexAgentCreateOperator(
            task_id="create_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
        )

        result = operator.execute(context={})

        mock_create_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            comment=None,
            profile=None,
            models=None,
            instructions=None,
            orchestration=None,
            tools=None,
            tool_resources=None,
            create_mode=CreateMode.ERROR_IF_EXISTS,
            timeout=600,
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
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "update_agent",
        autospec=True,
    )
    def test_execute(self, mock_update_agent):
        response = {}
        mock_update_agent.return_value = response

        operator = SnowflakeCortexAgentUpdateOperator(
            task_id="update_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
        )

        result = operator.execute(context={})

        mock_update_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            comment=None,
            profile=None,
            models=None,
            instructions=None,
            orchestration=None,
            tools=None,
            tool_resources=None,
            timeout=600,
        )
        assert result == response


class TestSnowflakeCortexAgentDeleteOperator:
    @mock.patch.object(
        SnowflakeCortexAgentHook,
        "delete_agent",
        autospec=True,
    )
    def test_execute(self, mock_delete_agent):
        """Test that the operator delegates deletion to the hook."""
        response = {"status": "deleted"}
        mock_delete_agent.return_value = response

        operator = SnowflakeCortexAgentDeleteOperator(
            task_id="delete_agent",
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
        )

        result = operator.execute(context={})

        mock_delete_agent.assert_called_once_with(
            mock.ANY,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            if_exists=False,
            timeout=600,
        )

        assert result == response
