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

from airflow.models.dag import DAG
from airflow.providers.snowflake.operators.snowflake_cortex_agent import (
    SnowflakeCortexAgentOperator,
)
from airflow.utils import timezone

TASK_ID = "run_agent"
CONN_ID = "snowflake_default"


class TestSnowflakeCortexAgentOperator:
    @mock.patch(
        "airflow.providers.snowflake.operators.snowflake_cortex_agent.SnowflakeCortexAgentHook.run_agent"
    )
    def test_execute(self, mock_run_agent):
        """Test that the operator delegates execution to the hook."""
        response = {"content": [{"type": "text", "text": "Hello"}]}
        mock_run_agent.return_value = response

        operator = SnowflakeCortexAgentOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=[
                {
                    "role": "user",
                    "content": "Hello",
                }
            ],
        )

        result = operator.execute(context={})

        mock_run_agent.assert_called_once_with(
            database="MY_DATABASE",
            schema="MY_SCHEMA",
            agent_name="my_agent",
            messages=[
                {
                    "role": "user",
                    "content": "Hello",
                }
            ],
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

    def test_template_fields(self):
        dag = DAG(
            dag_id="test_template_fields",
            start_date=timezone.datetime(2024, 1, 1),
        )

        operator = SnowflakeCortexAgentOperator(
            task_id=TASK_ID,
            dag=dag,
            database="{{ var.value.database }}",
            schema="{{ params.schema }}",
            agent_name="{{ dag_run.conf['agent_name'] }}",
            messages=[
                {
                    "role": "user",
                    "content": "{{ ds }}",
                }
            ],
        )

        assert operator.template_fields == (
            "database",
            "schema",
            "agent_name",
            "messages",
        )
