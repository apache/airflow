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
"""Revise a draft until a judge passes it, within a dollar and a token budget, using a task loop."""

from __future__ import annotations

# [START howto_loop_budget]
import logging
from decimal import Decimal

from pydantic import BaseModel, Field

from airflow.sdk import LoopDecision, dag, task, task_group

log = logging.getLogger(__name__)

THRESHOLD = 0.9
MAX_USD = Decimal("5.00")
MAX_TOKENS = 500_000

INCIDENT_NOTES = """\
09:12 UTC p95 latency on the orders API jumps from 180 ms to 4 s.
09:20 Checkout errors at 12% in eu-west. Other regions normal.
09:41 Cause: a connection pool limit lowered in this morning's config deploy.
09:55 Config rolled back. Error rate falling, 2% at 10:05.
Next update at 10:30 UTC.
"""

JUDGE_INSTRUCTIONS = """\
Score a customer status page update from 0 to 1 against this style guide, and give one
sentence of feedback on what to fix. Score 1 only if every rule holds.

1. Every fact matches the incident notes.
2. The first sentence says which customers are affected and how.
3. No internal terms: no metric names, config, deploys, connection pools or rollbacks.
4. At most 80 words.
5. It ends with the time of the next update.
"""


class Score(BaseModel):
    """The judge's verdict on one draft."""

    score: float = Field(ge=0, le=1)
    feedback: str


@dag(tags=["example"])
def example_agent_loop_budget():
    @task_group
    def refine():
        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You write status page updates for customers. Plain words, no internal jargon.",
            # Bounds one pass: the loop's own caps are only checked between passes.
            usage_limits={"total_tokens_limit": 20_000, "cost_limit": "0.50"},
        )
        def write_draft(*, loop):
            if loop.index == 1:
                return f"Write a status page update from these incident notes:\n{INCIDENT_NOTES}"
            return (
                f"Revise this status page update.\nFeedback: {loop.previous['feedback']}\n"
                f"Draft:\n{loop.previous['draft']}\nIncident notes:\n{INCIDENT_NOTES}"
            )

        @task.llm(
            llm_conn_id="pydanticai_default",
            output_type=Score,
            # A dict in XCom, so ``tally`` can read it under ``airflow dags test`` too.
            serialize_output=True,
            system_prompt=JUDGE_INSTRUCTIONS,
            usage_limits={"total_tokens_limit": 10_000, "cost_limit": "0.20"},
        )
        def judge(draft):
            return f"Incident notes:\n{INCIDENT_NOTES}\nUpdate:\n{draft}"

        @task
        def tally(draft, verdict, *, loop, ti):
            # Inside a loop, this pulls the usage of the current iteration only.
            usage = ti.xcom_pull(task_ids=["refine.write_draft", "refine.judge"], key="usage")
            spent = {"tokens": 0, "usd": "0"} if loop.index == 1 else loop.previous
            tokens = spent["tokens"] + sum(u["total_tokens"] for u in usage)
            costs = [u["cost"] for u in usage]
            if spent["usd"] is None:
                usd = None
            elif None in costs:
                # An unpriced call makes the dollar total unknown, not lower.
                log.warning("A model call had no price, so the dollar cap no longer applies to this loop.")
                usd = None
            else:
                usd = str(Decimal(spent["usd"]) + sum(map(Decimal, costs)))
            return {
                "draft": draft,
                "score": verdict["score"],
                "feedback": verdict["feedback"],
                "tokens": tokens,
                "usd": usd,
            }

        draft = write_draft()
        tally(draft, judge(draft))

    def good_enough(*, loop):
        result = loop.result
        usd = "cost unknown" if result["usd"] is None else f"${result['usd']} of ${MAX_USD}"
        progress = f"score {result['score']}; {usd}, {result['tokens']:,} of {MAX_TOKENS:,} tokens"
        if result["score"] >= THRESHOLD:
            return LoopDecision(stop=True, reason=f"passed: {progress}")
        if result["usd"] is not None and Decimal(result["usd"]) >= MAX_USD:
            return LoopDecision(stop=True, reason=f"dollar budget spent: {progress}")
        if result["tokens"] >= MAX_TOKENS:
            return LoopDecision(stop=True, reason=f"token budget spent: {progress}")
        return LoopDecision(stop=False, reason=progress)

    @task
    def publish(passes):
        last = passes[-1]
        usd = "cost unknown" if last["usd"] is None else f"${last['usd']}"
        log.info("%d passes, score %s, %s tokens, %s", len(passes), last["score"], last["tokens"], usd)
        return last["draft"]

    refinement = refine.loop(max_iterations=20, until=good_enough)
    publish(refinement["tally"].output)


example_agent_loop_budget()
# [END howto_loop_budget]
