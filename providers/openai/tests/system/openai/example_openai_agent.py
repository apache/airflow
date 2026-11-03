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

import pendulum

from airflow.providers.common.compat.sdk import dag
from airflow.providers.openai.operators.agent import OpenAIAgentSessionOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "openai"],
)
def example_openai_agent():
    # [START howto_operator_openai_agent]
    OpenAIAgentSessionOperator(
        task_id="run_agent",
        input="Explain how Airflow retries affect a task that calls an external API.",
        environment={"type": "none"},
        session_kwargs={
            "agent": {
                "model": "gpt-6-astra",
                "instructions": "Give a short explanation suitable for a data engineer.",
            }
        },
        deferrable=True,
        poll_interval=10,
        timeout=600,
    )
    # [END howto_operator_openai_agent]


example_dag = example_openai_agent()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(example_dag)
