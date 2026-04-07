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
Interactive natural language analysis of a survey CSV.

``example_llm_survey_interactive`` queries the `Airflow Community Survey 2025
<https://airflow.apache.org/survey/>`__ CSV using a five-step pipeline:

  1. **HITLEntryOperator** — human reviews and optionally edits the question.
  2. **LLMSQLQueryOperator** — translates the confirmed question into SQL.
  3. **AnalyticsOperator** — executes the SQL against the CSV via Apache
     DataFusion and returns the results as JSON.
  4. A ``@task`` function — extracts the data rows from the JSON payload.
  5. **ApprovalOperator** — human approves or rejects the query result.

Before running:

1. Create an LLM connection named ``pydanticai_default`` (or the value of
   ``LLM_CONN_ID`` below) for your chosen model provider.
2. Place the survey CSV at the path set by the ``SURVEY_CSV_PATH``
   environment variable, or update ``SURVEY_CSV_PATH`` below.
   A cleaned copy of the 2025 survey CSV (duplicate columns renamed, embedded
   newlines removed) is required — Apache DataFusion is strict about these.
"""

from __future__ import annotations

import datetime
import json
import os

from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig
from airflow.providers.common.sql.operators.analytics import AnalyticsOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# LLM provider connection (OpenAI, Anthropic, Vertex AI, etc.)
LLM_CONN_ID = "pydanticai_default"

# Path to the survey CSV.  Set the SURVEY_CSV_PATH environment variable to
# override — no code change needed when moving between environments.
SURVEY_CSV_PATH = os.environ.get(
    "SURVEY_CSV_PATH",
    "/opt/airflow/data/airflow-user-survey-2025.csv",
)
SURVEY_CSV_URI = f"file://{SURVEY_CSV_PATH}"

# Default question — the human can edit it in the first HITL step.
INTERACTIVE_PROMPT = "Which city had the highest number of respondents?"

# Schema context for LLMSQLQueryOperator.
# Lists the analytically relevant columns from the 2025 survey CSV (168 total).
# All column names must be quoted in SQL because they contain spaces and
# punctuation.
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
  "What city do you currently reside in?"                                                     TEXT
  "How many years of experience do you have with Airflow?"                                    TEXT
  "Which of the following is your company's primary cloud provider for Airflow?"              TEXT
  "How many people work at your company?"                                                     TEXT
  "How many people at your company directly work on data?"                                    TEXT
  "How many people at your company use Airflow?"                                              TEXT
  "How likely are you to recommend Apache Airflow?"                                           TEXT
  "Are you using AI/LLM (ChatGPT/Cursor/Claude etc) to assist you in writing Airflow code?"  TEXT
"""

survey_datasource = DataSourceConfig(
    conn_id="",
    table_name="survey",
    uri=SURVEY_CSV_URI,
    format="csv",
)


# ---------------------------------------------------------------------------
# DAG 1: Interactive survey question example
# ---------------------------------------------------------------------------


# [START example_llm_survey_interactive]
@dag(schedule=None)
def example_llm_survey_interactive():
    """
    Ask a natural language question about the survey with human review at each end.

    Task graph::

        prompt_confirmation (HITLEntryOperator)
            → generate_sql (LLMSQLQueryOperator)
            → run_query (AnalyticsOperator)
            → extract_data (@task)
            → result_confirmation (ApprovalOperator)

    The first HITL step lets the analyst review and optionally reword the
    question before it reaches the LLM.  The final HITL step presents the
    query result for approval or rejection.
    """

    # ------------------------------------------------------------------
    # Step 1: Prompt confirmation — review or edit the question.
    # ------------------------------------------------------------------
    prompt_confirmation = HITLEntryOperator(
        task_id="prompt_confirmation",
        subject="Review the survey analysis question",
        params={
            "prompt": Param(
                INTERACTIVE_PROMPT,
                type="string",
                description="The natural language question to answer via SQL",
            )
        },
        response_timeout=datetime.timedelta(hours=1),
    )

    # ------------------------------------------------------------------
    # Step 2: SQL generation — LLM translates the confirmed question.
    # ------------------------------------------------------------------
    generate_sql = LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt="{{ ti.xcom_pull(task_ids='prompt_confirmation')['params_input']['prompt'] }}",
        llm_conn_id=LLM_CONN_ID,
        datasource_config=survey_datasource,
        schema_context=SURVEY_SCHEMA,
    )

    # ------------------------------------------------------------------
    # Step 3: SQL execution via Apache DataFusion.
    # ------------------------------------------------------------------
    run_query = AnalyticsOperator(
        task_id="run_query",
        datasource_configs=[survey_datasource],
        queries=["{{ ti.xcom_pull(task_ids='generate_sql') }}"],
        result_output_format="json",
    )

    # ------------------------------------------------------------------
    # Step 4: Extract data rows from the JSON result.
    # AnalyticsOperator returns [{"query": "...", "data": [...]}, ...]
    # This step strips the query field so only the rows reach the reviewer.
    # ------------------------------------------------------------------
    @task
    def extract_data(raw: str) -> str:
        results = json.loads(raw)
        data = [row for item in results for row in item["data"]]
        return json.dumps(data, indent=2)

    result_data = extract_data(run_query.output)

    # ------------------------------------------------------------------
    # Step 5: Result confirmation — approve or reject the query result.
    # ------------------------------------------------------------------
    result_confirmation = ApprovalOperator(
        task_id="result_confirmation",
        subject="Review the survey query result",
        body="{{ ti.xcom_pull(task_ids='extract_data') }}",
        response_timeout=datetime.timedelta(hours=1),
    )

    prompt_confirmation >> generate_sql >> run_query >> result_data >> result_confirmation


# [END example_llm_survey_interactive]

example_llm_survey_interactive()
