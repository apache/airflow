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
"""
Example use of SnowflakeCortexAgentOperator.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake_cortex_agent import CreateMode
from airflow.providers.snowflake.operators.snowflake_cortex_agent import (
    SnowflakeCortexAgentCreateOperator,
    SnowflakeCortexAgentDeleteOperator,
    SnowflakeCortexAgentOperator,
    SnowflakeCortexAgentUpdateOperator,
)

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
DAG_ID = "example_snowflake_cortex_agent"

DATABASE = "DEFAULT_DATABASE"
SCHEMA = "DEFAULT_SCHEMA"
AGENT_NAME = "default_agent"

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_snowflake_cortex_agent_create]
    create_agent = SnowflakeCortexAgentCreateOperator(
        task_id="create_agent",
        database=DATABASE,
        schema=SCHEMA,
        agent_name=AGENT_NAME,
        comment="Created by Airflow",
        instructions={
            "response": "Respond in a friendly and concise manner.",
        },
        create_mode=CreateMode.ERROR_IF_EXISTS,
    )
    # [END howto_operator_snowflake_cortex_agent_create]

    # [START howto_operator_snowflake_cortex_agent_update]
    update_agent = SnowflakeCortexAgentUpdateOperator(
        task_id="update_agent",
        database=DATABASE,
        schema=SCHEMA,
        agent_name=AGENT_NAME,
        comment="Updated by Airflow",
        instructions={
            "response": "Respond in one sentence.",
        },
    )
    # [END howto_operator_snowflake_cortex_agent_update]

    # [START howto_operator_snowflake_cortex_agent]
    run_agent = SnowflakeCortexAgentOperator(
        task_id="run_agent",
        database=DATABASE,
        schema=SCHEMA,
        agent_name=AGENT_NAME,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "What can you help me with?",
                    }
                ],
            }
        ],
    )
    # [END howto_operator_snowflake_cortex_agent]

    # [START howto_operator_snowflake_cortex_agent_delete]
    delete_agent = SnowflakeCortexAgentDeleteOperator(
        task_id="delete_agent",
        database=DATABASE,
        schema=SCHEMA,
        agent_name=AGENT_NAME,
        if_exists=True,
    )
    # [END howto_operator_snowflake_cortex_agent_delete]

    create_agent >> update_agent >> run_agent >> delete_agent


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
