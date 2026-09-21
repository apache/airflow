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
Example Dags for ``LLMBatchOperator`` and ``@task.llm_batch``.

Each Dag submits a handful of product reviews to a provider batch API, waits
for the batch in deferrable mode, lands the results as JSONL on object storage
and reads them back in a downstream task. ``pydanticai_default`` is a
``pydanticai`` connection whose password is the provider API key.
"""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field

from airflow.providers.common.ai.operators.llm_batch import LLMBatchOperator
from airflow.providers.common.compat.sdk import ObjectStoragePath, dag, task

REVIEWS = [
    "Absolutely loved it, would buy again.",
    "Broke after one use, very disappointed.",
    "Does what it says on the box. Nothing special.",
    "Shipping took three weeks but the product itself is great.",
]

# One directory per Dag run so retries of the same run re-attach to their batch
# while different runs never share results. ``{{ ts }}`` would break re-attachment.
RESULT_ROOT = "s3://my-bucket/llm-batch/{{ dag.dag_id }}/{{ run_id }}"


# [START howto_operator_llm_batch_structured_output_class]
class Sentiment(BaseModel):
    """Structured output requested from every request in the batch."""

    label: Literal["positive", "negative", "neutral"]
    confidence: float = Field(ge=0, le=1)
    reason: str


# [END howto_operator_llm_batch_structured_output_class]


# [START howto_operator_llm_batch_read_results]
@task
def summarize_manifest(manifest: dict) -> dict[str, int]:
    """Read the landed JSONL rows back; the XCom value is only the manifest."""
    rows = [
        json.loads(line)
        for line in ObjectStoragePath(manifest["result_uri"]).read_text().splitlines()
        if line
    ]
    by_label: dict[str, int] = {}
    for row in rows:
        if row["status"] == "success":
            by_label[row["output"]["label"]] = by_label.get(row["output"]["label"], 0) + 1
        else:
            # ``error`` (provider-side failure) or ``invalid_output`` (schema mismatch,
            # original text kept in ``raw_output``); the manifest's ``counts`` has the totals.
            print(f"request {row['index']} -> {row['status']}: {row['error'] or row['raw_output']}")
    return by_label


# [END howto_operator_llm_batch_read_results]


# [START howto_operator_llm_batch_basic]
@dag(tags=["example"])
def example_llm_batch_operator():
    classify = LLMBatchOperator(
        task_id="classify_reviews",
        requests=[f"Review: {review!r}" for review in REVIEWS],
        result_path=f"{RESULT_ROOT}/classify",
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-5-mini",
        system_prompt="Classify the sentiment of this product review.",
        output_type=Sentiment,
        deferrable=True,
    )
    summarize_manifest(classify.output)


# [END howto_operator_llm_batch_basic]

example_llm_batch_operator()


# [START howto_decorator_llm_batch]
@dag(tags=["example"])
def example_llm_batch_decorator():
    @task.llm_batch(
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-sonnet-4-5",
        result_path=f"{RESULT_ROOT}/classify",
        system_prompt="Classify the sentiment of this product review.",
        output_type=Sentiment,
        deferrable=True,
    )
    def build_review_prompts(reviews: list[str]) -> list[str]:
        return [f"Review: {review!r}" for review in reviews]

    summarize_manifest(build_review_prompts(REVIEWS))


# [END howto_decorator_llm_batch]

example_llm_batch_decorator()


# [START howto_operator_llm_batch_provider_params]
@dag(tags=["example"])
def example_llm_batch_provider_params():
    # ``request_params`` is merged into every request body as-is, so any parameter
    # the provider's chat endpoint accepts can go here: OpenAI chat-completions keys
    # for an ``openai:`` model, Anthropic Messages keys for an ``anthropic:`` model.
    # ``max_tokens`` is translated per provider (``max_completion_tokens`` on OpenAI).
    LLMBatchOperator(
        task_id="extract_keywords_openai",
        requests=[f"Review: {review!r}" for review in REVIEWS],
        result_path=f"{RESULT_ROOT}/keywords-openai",
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-5-mini",
        system_prompt="Extract up to three keywords from the review.",
        output_type=list[str],
        max_tokens=2048,
        request_params={"reasoning_effort": "low", "user": "reviews-pipeline"},
        fail_on_partial_error=True,
        deferrable=True,
    )

    LLMBatchOperator(
        task_id="extract_keywords_anthropic",
        requests=[f"Review: {review!r}" for review in REVIEWS],
        result_path=f"{RESULT_ROOT}/keywords-anthropic",
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-sonnet-4-5",
        system_prompt="Extract up to three keywords from the review.",
        output_type=list[str],
        request_params={"temperature": 0.2, "metadata": {"user_id": "reviews-pipeline"}},
        deferrable=True,
    )


# [END howto_operator_llm_batch_provider_params]

example_llm_batch_provider_params()


# [START howto_operator_llm_batch_per_request]
@dag(tags=["example"])
def example_llm_batch_per_request_overrides():
    @task.llm_batch(
        llm_conn_id="pydanticai_default",
        model_id="openai:gpt-4.1-mini",
        result_path=f"{RESULT_ROOT}/summaries",
        system_prompt="Summarize the review in one short sentence.",
        max_tokens=60,
        deferrable=True,
    )
    def build_summary_requests() -> list[dict]:
        # A request dict overrides the batch-level system_prompt, max_tokens and
        # body params for that request only. ``model`` can also be overridden per
        # request on Anthropic; OpenAI requires one model per batch.
        return [
            {"prompt": f"Review: {REVIEWS[0]!r}"},
            {
                "prompt": f"Review: {REVIEWS[1]!r}",
                "system_prompt": "Summarize the review in one short sentence, in French.",
            },
            {"prompt": f"Review: {REVIEWS[2]!r}", "max_tokens": 120, "params": {"temperature": 0.9}},
        ]

    build_summary_requests()


# [END howto_operator_llm_batch_per_request]

example_llm_batch_per_request_overrides()
