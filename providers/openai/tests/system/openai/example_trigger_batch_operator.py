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

from typing import Any, Literal

from airflow.decorators import dag, task

OPENAI_CONN_ID = "openai_default"

POKEMONS = [
    "pikachu",
    "charmander",
    "bulbasaur",
]


@dag(
    schedule=None,
    catchup=False,
)
def openai_batch_chat_completions():
    @task
    def generate_messages(pokemon, **context) -> list[dict[str, Any]]:
        return [{"role": "user", "content": f"Describe the info about {pokemon}?"}]

    @task
    def batch_upload(messages_batch, **context) -> str:
        import tempfile
        import uuid

        from pydantic import BaseModel, Field

        from airflow.providers.openai.hooks.openai import OpenAIHook

        class RequestBody(BaseModel):
            model: str
            messages: list[dict[str, Any]]
            max_tokens: int = Field(default=1000)

        class BatchModel(BaseModel):
            custom_id: str
            method: Literal["POST"]
            url: Literal["/v1/chat/completions"]
            body: RequestBody

        model = "gpt-4o-mini"
        max_tokens = 1000
        hook = OpenAIHook(conn_id=OPENAI_CONN_ID)
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as file:
            for messages in messages_batch:
                file.write(
                    BatchModel(
                        custom_id=str(uuid.uuid4()),
                        method="POST",
                        url="/v1/chat/completions",
                        body=RequestBody(
                            model=model,
                            max_tokens=max_tokens,
                            messages=messages,
                        ),
                    ).model_dump_json()
                    + "\n"
                )
        batch_file = hook.upload_file(file.name, purpose="batch")
        return batch_file.id

    @task
    def cleanup_batch_output_file(batch_id, **context):
        from airflow.providers.openai.hooks.openai import OpenAIHook

        hook = OpenAIHook(conn_id=OPENAI_CONN_ID)
        batch = hook.get_batch(batch_id)
        if batch.output_file_id:
            hook.delete_file(batch.output_file_id)

    messages = generate_messages.expand(pokemon=POKEMONS)
    batch_file_id = batch_upload(messages_batch=messages)

    # [START howto_operator_openai_trigger_operator]
    from airflow.providers.openai.operators.openai import OpenAITriggerBatchOperator

    batch_id = OpenAITriggerBatchOperator(
        task_id="batch_operator_deferred",
        conn_id=OPENAI_CONN_ID,
        file_id=batch_file_id,
        endpoint="/v1/chat/completions",
        deferrable=True,
    )
    # [END howto_operator_openai_trigger_operator]
    cleanup_batch_output = cleanup_batch_output_file(
        batch_id="{{ ti.xcom_pull(task_ids='batch_operator_deferred', key='return_value') }}"
    )
    batch_id >> cleanup_batch_output


openai_batch_chat_completions()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
