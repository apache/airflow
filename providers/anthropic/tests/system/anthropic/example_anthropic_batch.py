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

from airflow.sdk import dag, task

ANTHROPIC_CONN_ID = "anthropic_default"
MODEL = "claude-opus-4-8"

POKEMON = ["pikachu", "charmander", "bulbasaur"]


@dag(schedule=None, catchup=False)
def anthropic_batch_messages():
    @task
    def build_requests(names: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "custom_id": name,
                "params": {
                    "model": MODEL,
                    "max_tokens": 256,
                    "messages": [{"role": "user", "content": f"Describe {name} in one sentence."}],
                },
            }
            for name in names
        ]

    @task
    def collect_results(batch_id: str) -> dict[str, str]:
        # Results stream from the API unordered; key them by custom_id. For large
        # batches, persist to object storage instead of returning via XCom.
        from airflow.providers.anthropic.hooks.anthropic import AnthropicHook

        hook = AnthropicHook(conn_id=ANTHROPIC_CONN_ID)
        summaries: dict[str, str] = {}
        for entry in hook.stream_batch_results(batch_id):
            if entry.result.type == "succeeded":
                text = next((b.text for b in entry.result.message.content if b.type == "text"), "")
                summaries[entry.custom_id] = text
        return summaries

    requests = build_requests(POKEMON)

    # [START howto_operator_anthropic_batch]
    from airflow.providers.anthropic.operators.batch import AnthropicBatchOperator

    run_batch = AnthropicBatchOperator(
        task_id="run_batch",
        conn_id=ANTHROPIC_CONN_ID,
        requests=requests,
        deferrable=True,
    )
    # [END howto_operator_anthropic_batch]

    collect_results(batch_id=run_batch.output)


anthropic_batch_messages()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
