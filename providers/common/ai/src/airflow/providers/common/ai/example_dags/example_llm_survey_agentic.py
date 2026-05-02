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
Multi-query synthesis -- an agentic survey analysis pattern.

Demonstrates how Dynamic Task Mapping turns a multi-dimensional research
question into a fan-out / fan-in pipeline that is observable, retryable,
and auditable at each step.

**Question:** *"What does a typical Airflow deployment look like for
practitioners who actively use AI tools in their workflow?"*

This question cannot be answered with a single SQL query.  It requires
querying four independent dimensions -- executor type, deployment method,
cloud provider, and Airflow version -- all filtered to respondents who use
AI tools to write Airflow code.  The results are then synthesized by a
second LLM call into a single narrative characterization.

``example_llm_survey_agentic`` (manual trigger):

.. code-block:: text

    decompose_question (@task)
        → generate_sql  (LLMSQLQueryOperator, mapped ×4)
        → wrap_query    (@task, mapped ×4)
        → run_query     (AnalyticsOperator, mapped ×4)
        → collect_results (@task)
        → synthesize_answer (LLMOperator)
        → result_confirmation (ApprovalOperator)

**What this makes visible that an agent harness hides:**

* Each sub-query is a named, logged task instance -- not a hidden tool call.
* If the cloud-provider query fails, only that mapped instance retries;
  the other three results are preserved in XCom.
* The synthesis step's inputs are fully auditable XCom values -- not an
  opaque continuation of an LLM reasoning loop.

Before running:

1. Create an LLM connection named ``pydanticai_default`` (or the value of
   ``LLM_CONN_ID``) for your chosen model provider.
2. Place the cleaned survey CSV at the path set by ``SURVEY_CSV_PATH``.
"""

from __future__ import annotations

import datetime
import json
import os

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig
from airflow.providers.common.sql.operators.analytics import AnalyticsOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "pydanticai_default"

SURVEY_CSV_PATH = os.environ.get(
    "SURVEY_CSV_PATH",
    "/opt/airflow/data/airflow-user-survey-2025.csv",
)
SURVEY_CSV_URI = f"file://{SURVEY_CSV_PATH}"

# Schema context for LLMSQLQueryOperator.
# All column names must be quoted in SQL because they contain spaces and punctuation.
SURVEY_SCHEMA = """
Table: survey
Key columns (quote all names in SQL):
  "How important is Airflow to your business?"                                                TEXT
  "Which version of Airflow do you currently use?"                                            TEXT
  "CeleryExecutor"                                                                            TEXT
  "KubernetesExecutor"                                                                        TEXT
  "LocalExecutor"                                                                             TEXT
  "How do you deploy Airflow?"                                                                TEXT
  "What best describes your current occupation?"                                              TEXT
  "What industry do you currently work in?"                                                   TEXT
  "How many years of experience do you have with Airflow?"                                    TEXT
  "Which of the following is your company's primary cloud provider for Airflow?"              TEXT
  "How many people work at your company?"                                                     TEXT
  "How many people at your company directly work on data?"                                    TEXT
  "How many people at your company use Airflow?"                                              TEXT
  "How likely are you to recommend Apache Airflow?"                                           TEXT
  "Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"  TEXT
"""

survey_datasource = DataSourceConfig(
    conn_id="",  # No connection needed for local file-based sources
    table_name="survey",
    uri=SURVEY_CSV_URI,
    format="csv",
)

# Dimension labels -- order must match the sub-questions returned by decompose_question.
DIMENSION_KEYS = ["executor", "deployment", "cloud", "airflow_version"]

SQL_SYSTEM_PROMPT = """\
You are a SQL analysis agent working on the table "survey".
Always quote all table and column names with double quotes.

For the AI usage filter column
"Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?":
- Treat affirmative free text (yes, sometimes, occasionally, rarely, often, regularly) as AI users.
- Treat explicit negatives (no, never) as non-users.
- Exclude blank, NULL, and ambiguous responses from the filtered set."""

SYNTHESIS_SYSTEM_PROMPT = """\
You are a data analyst summarizing survey results about Apache Airflow practitioners.
Write in plain, concise language suitable for a technical audience.
Focus on patterns and proportions rather than raw counts."""


# ---------------------------------------------------------------------------
# DAG: Agentic multi-query synthesis
# ---------------------------------------------------------------------------


# [START example_llm_survey_agentic]
@dag
def example_llm_survey_agentic():
    """
    Fan-out across four survey dimensions, then synthesize into a single narrative.

    Task graph::

        decompose_question (@task)
            → generate_sql  (LLMSQLQueryOperator ×4, via Dynamic Task Mapping)
            → wrap_query    (@task ×4)
            → run_query     (AnalyticsOperator ×4, via Dynamic Task Mapping)
            → collect_results (@task)
            → synthesize_answer (LLMOperator)
            → result_confirmation (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Decompose the high-level question into sub-questions,
    # one per dimension.  Each string becomes one mapped task instance
    # in the next step.
    # ------------------------------------------------------------------
    @task
    def decompose_question() -> list[str]:
        return [
            """\
Among respondents who use AI/LLM tools to write Airflow code,
what executor types (CeleryExecutor, KubernetesExecutor, LocalExecutor)
are most commonly enabled? Count an executor as enabled only if the
column value is clearly affirmative. Treat blank, NULL, and negative
values as not enabled. Return the count per executor type.""",
            """\
Among respondents who use AI/LLM tools to write Airflow code,
how do they deploy Airflow? Return the count per deployment method.""",
            """\
Among respondents who use AI/LLM tools to write Airflow code,
which cloud providers are most commonly used for Airflow?
Return the count per cloud provider.""",
            """\
Among respondents who use AI/LLM tools to write Airflow code,
what version of Airflow are they currently running?
Return the count per version.""",
        ]

    sub_questions = decompose_question()

    # ------------------------------------------------------------------
    # Step 2: Generate SQL for each sub-question in parallel.
    # LLMSQLQueryOperator is expanded over the sub-question list --
    # four mapped instances, each translating one natural-language
    # question into validated SQL.
    # ------------------------------------------------------------------
    generate_sql = LLMSQLQueryOperator.partial(
        task_id="generate_sql",
        llm_conn_id=LLM_CONN_ID,
        datasource_config=survey_datasource,
        schema_context=SURVEY_SCHEMA,
        system_prompt=SQL_SYSTEM_PROMPT,
    ).expand(prompt=sub_questions)

    # ------------------------------------------------------------------
    # Step 3: Wrap each SQL string into a single-element list.
    # AnalyticsOperator expects queries: list[str]; this step bridges
    # the scalar output of LLMSQLQueryOperator to that interface.
    # ------------------------------------------------------------------
    @task
    def wrap_query(sql: str) -> list[str]:
        return [sql]

    wrapped_queries = wrap_query.expand(sql=generate_sql.output)

    # ------------------------------------------------------------------
    # Step 4: Execute each SQL against the survey CSV via DataFusion.
    # Four mapped instances run in parallel.  If one fails, only that
    # instance retries -- the other three hold their XCom results.
    # ------------------------------------------------------------------
    run_query = AnalyticsOperator.partial(
        task_id="run_query",
        datasource_configs=[survey_datasource],
        result_output_format="json",
    ).expand(queries=wrapped_queries)

    # ------------------------------------------------------------------
    # Step 5: Collect all four JSON results and label them by dimension.
    # The default trigger rule (all_success) ensures synthesis only runs
    # when the complete picture is available.
    # ------------------------------------------------------------------
    @task
    def collect_results(results: list[str]) -> dict:
        # Airflow preserves index order for mapped task outputs, so zip is safe here:
        # results[i] corresponds to the mapped instance at index i, which matches
        # the sub-question at DIMENSION_KEYS[i].
        labeled: dict[str, list] = {}
        for key, raw in zip(DIMENSION_KEYS, results):
            items = json.loads(raw)
            data = [row for item in items for row in item["data"]]
            labeled[key] = data
        return labeled

    collected = collect_results(run_query.output)

    # ------------------------------------------------------------------
    # Step 6: Synthesize the four labeled result sets into a narrative.
    # This is the second LLM call -- the first four generated SQL,
    # this one interprets the results.  Inputs are fully visible in XCom.
    # ------------------------------------------------------------------
    synthesize_answer = LLMOperator(
        task_id="synthesize_answer",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Given these four independent survey query results about practitioners
who use AI tools to write Airflow code, write a 2-3 sentence
characterization of what a typical Airflow deployment looks like for
this group.

Results: {{ ti.xcom_pull(task_ids='collect_results') }}""",
    )
    collected >> synthesize_answer

    # ------------------------------------------------------------------
    # Step 7: Human reviews the synthesized narrative before the DAG ends.
    # ------------------------------------------------------------------
    result_confirmation = ApprovalOperator(  # noqa: F841
        task_id="result_confirmation",
        subject="Review the synthesized survey analysis",
        body=synthesize_answer.output,
        response_timeout=datetime.timedelta(hours=1),
    )


# [END example_llm_survey_agentic]

example_llm_survey_agentic()
