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
"""Example DAGs demonstrating LLMOperator and @task.llm usage."""

from __future__ import annotations

from pydantic import BaseModel

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_llm_basic]
@dag
def example_llm_operator():
    LLMOperator(
        task_id="summarize",
        prompt="Summarize the key findings from the Q4 earnings report.",
        llm_conn_id="pydantic_ai_default",
        system_prompt="You are a financial analyst. Be concise.",
    )


# [END howto_operator_llm_basic]

example_llm_operator()


# [START howto_operator_llm_structured]
@dag
def example_llm_operator_structured():
    class Entities(BaseModel):
        names: list[str]
        locations: list[str]

    LLMOperator(
        task_id="extract_entities",
        prompt="Extract all named entities from the article.",
        llm_conn_id="pydantic_ai_default",
        system_prompt="Extract named entities.",
        output_type=Entities,
    )


# [END howto_operator_llm_structured]

example_llm_operator_structured()


# [START howto_operator_llm_agent_params]
@dag
def example_llm_operator_agent_params():
    LLMOperator(
        task_id="creative_writing",
        prompt="Write a haiku about data pipelines.",
        llm_conn_id="pydantic_ai_default",
        system_prompt="You are a creative writer.",
        agent_params={"model_settings": {"temperature": 0.9}, "retries": 3},
    )


# [END howto_operator_llm_agent_params]

example_llm_operator_agent_params()


# [START howto_decorator_llm]
@dag
def example_llm_decorator():
    @task.llm(llm_conn_id="pydantic_ai_default", system_prompt="Summarize concisely.")
    def summarize(text: str):
        return f"Summarize this article: {text}"

    summarize("Apache Airflow is a platform for programmatically authoring...")


# [END howto_decorator_llm]

example_llm_decorator()


# [START howto_decorator_llm_structured]
@dag
def example_llm_decorator_structured():
    class Entities(BaseModel):
        names: list[str]
        locations: list[str]

    @task.llm(
        llm_conn_id="pydantic_ai_default",
        system_prompt="Extract named entities.",
        output_type=Entities,
    )
    def extract(text: str):
        return f"Extract entities from: {text}"

    extract("Alice visited Paris and met Bob in London.")


# [END howto_decorator_llm_structured]

example_llm_decorator_structured()
