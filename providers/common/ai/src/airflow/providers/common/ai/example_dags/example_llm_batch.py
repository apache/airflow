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
"""Example Dags demonstrating LLMBatchOperator and @task.llm_batch usage."""

from __future__ import annotations

from pydantic import BaseModel

from airflow.providers.common.ai.operators.llm_batch import LLMBatchOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_llm_batch_structured_output_class]
class Sentiment(BaseModel):
    """Structured output requested from every request in the batch."""

    label: str
    confidence: float


# [END howto_operator_llm_batch_structured_output_class]


# [START howto_operator_llm_batch_basic]
@dag(tags=["example"])
def example_llm_batch_operator():
    LLMBatchOperator(
        task_id="classify_reviews",
        requests=[
            "Review: 'Absolutely loved it, would buy again.'",
            "Review: 'Broke after one use, very disappointed.'",
        ],
        result_path="s3://my-bucket/llm-batch/classify-reviews",
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-5",
        system_prompt="Classify the sentiment of this product review.",
        output_type=Sentiment,
    )


# [END howto_operator_llm_batch_basic]

example_llm_batch_operator()


# [START howto_decorator_llm_batch]
@dag(tags=["example"])
def example_llm_batch_decorator():
    @task.llm_batch(
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-5",
        result_path="s3://my-bucket/llm-batch/classify-reviews-taskflow",
        system_prompt="Classify the sentiment of this product review.",
        output_type=Sentiment,
    )
    def build_review_prompts(reviews: list[str]) -> list[str]:
        return [f"Review: {review!r}" for review in reviews]

    build_review_prompts(
        [
            "Absolutely loved it, would buy again.",
            "Broke after one use, very disappointed.",
        ]
    )


# [END howto_decorator_llm_batch]

example_llm_batch_decorator()
