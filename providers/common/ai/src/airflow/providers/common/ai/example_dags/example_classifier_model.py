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
Example DAG using a classifier model to route an incident.

A classifier model answers typed questions and cannot write text, so it suits a branch
(pick one of these task ids) and a classification (pick one of these labels), and nothing
that produces prose. See the "Classifier models" guide in this provider's documentation.

Prerequisites:
  - ``pip install 'apache-airflow-providers-common-ai[typesafe]'``
  - Connection ``jev_default`` with ``conn_type='pydanticai'``, ``password=<API key>``,
    ``extra='{"model": "typesafe:jev-1.13.0"}'``
"""

from __future__ import annotations

from typing import Literal

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.compat.sdk import dag, task

# Clear-cut: the error names the missing permission, so one remediation fits and the others
# do not. Branch options a classifier model can separate are the ones to give it.
PERMISSION_FAILURE = (
    "Task export_report failed: botocore.exceptions.ClientError: An error occurred "
    "(AccessDenied) when calling the PutObject operation: the task role "
    "airflow-worker lacks s3:PutObject on arn:aws:s3:::reports-prod/daily/."
)

# Answerable but not decisively: a classifier model lands on ``resource`` here with roughly
# two thirds of the probability, which is enough to be worth recording and not enough to act
# on unattended. That is the case the confidence gate below exists for.
UNDERDETERMINED_INCIDENT = (
    "Task load_orders failed: psycopg2.OperationalError: connection refused to "
    "warehouse.internal:5432. Two other Dags writing to the same warehouse failed in the "
    "last ten minutes. A Terraform apply touching the database security group merged "
    "four hours ago."
)

# Acting automatically deserves a higher bar than flagging for review, so the two use
# different thresholds rather than one shared number.
ACT_ABOVE = 0.8
REVIEW_ABOVE = 0.5


# [START howto_classifier_model_branch]
@dag(tags=["example", "classifier"])
def example_classifier_model_branch():
    """Route the incident. The model id is the only thing that makes this a classifier."""
    route = LLMBranchOperator(
        task_id="route_failure",
        prompt=PERMISSION_FAILURE,
        llm_conn_id="jev_default",
        model_id="typesafe:jev-1.13.0",
        system_prompt="Pick the remediation that addresses the cause, not the symptom.",
    )

    @task
    def grant_bucket_write():
        return "Adding s3:PutObject for the task role"

    @task
    def restore_deleted_bucket():
        return "Recreating the destination bucket"

    @task
    def wait_and_retry():
        return "Treating this as transient"

    route >> [grant_bucket_write(), restore_deleted_bucket(), wait_and_retry()]


# [END howto_classifier_model_branch]

example_classifier_model_branch()


# [START howto_classifier_model_confidence]
@dag(tags=["example", "classifier"])
def example_classifier_model_confidence():
    """Classify a failure and escalate when the model says it does not know.

    The branch Dag above cannot do this: ``LLMBranchOperator`` takes the branch inside the
    operator, before any task can read the confidence.
    """

    @task
    def classify(log_line: str) -> dict:
        agent = PydanticAIHook(llm_conn_id="jev_default").create_agent(
            output_type=Literal["transient", "resource", "permanent"],
            instructions="Classify why this Airflow task failed.",
        )
        result = agent.run_sync(log_line)
        # Confidence is reported per output field; a bare output type lands under
        # "response". A bounded ``float`` output would report none at all -- there the
        # probability is the answer -- so this ``or 0.0`` would read as no confidence
        # rather than as a missing one. No operator surfaces this on XCom by default.
        details = result.response.provider_details or {}
        return {
            "category": result.output,
            "confidence": (details.get("confidence") or {}).get("response"),
        }

    @task
    def act(classification: dict) -> str:
        confidence = classification["confidence"] or 0.0
        if confidence >= ACT_ABOVE:
            return f"Remediating {classification['category']} automatically"
        if confidence >= REVIEW_ABOVE:
            return f"Filing {classification['category']} for review"
        return "Paging a human: the classification was a coin flip"

    act(classify(UNDERDETERMINED_INCIDENT))


# [END howto_classifier_model_confidence]

example_classifier_model_confidence()
