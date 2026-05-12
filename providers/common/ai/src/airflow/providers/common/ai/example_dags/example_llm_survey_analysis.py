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
Natural language analysis of a survey CSV -- interactive and scheduled variants.

Both DAGs query the `Airflow Community Survey 2025
<https://airflow.apache.org/survey/>`__ CSV using
:class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`
and :class:`~airflow.providers.common.sql.operators.analytics.AnalyticsOperator`.

**example_llm_survey_interactive** (five tasks, manual trigger) adds
human-in-the-loop review at both ends of the pipeline: HITLEntryOperator,
LLMSQLQueryOperator, AnalyticsOperator, a ``@task`` extraction step, and
ApprovalOperator.

**example_llm_survey_scheduled** (seven tasks, runs monthly) downloads the CSV,
validates its schema, generates and executes SQL, then emails or logs the result.
No human review steps -- suitable for recurring reporting or dashboards.

Before running either DAG:

1. Create an LLM connection named ``pydanticai_default`` (or the value of
   ``LLM_CONN_ID`` below) for your chosen model provider.
2. Place the survey CSV at the path set by the ``SURVEY_CSV_PATH``
   environment variable, or update ``SURVEY_CSV_PATH`` below.
   A cleaned copy of the 2025 survey CSV (duplicate columns renamed, embedded
   newlines removed) is required -- Apache DataFusion is strict about these.
"""

from __future__ import annotations

import csv as csv_mod
import datetime
import json
import os

from airflow.providers.common.ai.operators.llm_schema_compare import LLMSchemaCompareOperator
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig
from airflow.providers.common.sql.operators.analytics import AnalyticsOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# LLM provider connection (OpenAI, Anthropic, Vertex AI, etc.)
LLM_CONN_ID = "pydanticai_default"

# HTTP connection pointing at https://airflow.apache.org (scheduled DAG only).
# Create a connection with host=https://airflow.apache.org, no auth required.
AIRFLOW_WEBSITE_CONN_ID = "airflow_website"

# Endpoint path for the survey CSV download, relative to the HTTP connection base URL.
SURVEY_CSV_ENDPOINT = "/survey/airflow-user-survey-2025.csv"

# Path to the survey CSV.  Set the SURVEY_CSV_PATH environment variable to
# override -- no code change needed when moving between environments.
SURVEY_CSV_PATH = os.environ.get(
    "SURVEY_CSV_PATH",
    "/opt/airflow/data/airflow-user-survey-2025.csv",
)
SURVEY_CSV_URI = f"file://{SURVEY_CSV_PATH}"

# Path where the reference schema CSV is written at runtime (scheduled DAG only).
REFERENCE_CSV_PATH = os.environ.get(
    "REFERENCE_CSV_PATH",
    "/opt/airflow/data/airflow-user-survey-2025-reference.csv",
)
REFERENCE_CSV_URI = f"file://{REFERENCE_CSV_PATH}"

# SMTP connection for the result notification step (scheduled DAG only).
# Set to None to skip email and log the result instead.
SMTP_CONN_ID = os.environ.get("SMTP_CONN_ID", None)
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", None)

# Default question for the interactive DAG -- the human can edit it in the first HITL step.
INTERACTIVE_PROMPT = (
    "How does AI tool usage for writing Airflow code compare between Airflow 3 users and Airflow 2 users?"
)

# Fixed question for the scheduled DAG -- runs unattended on every trigger.
SCHEDULED_PROMPT = "What is the breakdown of respondents by Airflow version currently in use?"

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
    conn_id="",  # No connection needed for local file-based sources
    table_name="survey",
    uri=SURVEY_CSV_URI,
    format="csv",
)

reference_datasource = DataSourceConfig(
    conn_id="",  # No connection needed for local file-based sources
    table_name="survey_reference",
    uri=REFERENCE_CSV_URI,
    format="csv",
)


# ---------------------------------------------------------------------------
# DAG 1: Interactive survey question example
# ---------------------------------------------------------------------------


# [START example_llm_survey_interactive]
@dag
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
    # Step 1: Prompt confirmation -- review or edit the question.
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
    # Step 2: SQL generation -- LLM translates the confirmed question.
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
    # Step 5: Result confirmation -- approve or reject the query result.
    # ------------------------------------------------------------------
    result_confirmation = ApprovalOperator(  # noqa: F841
        task_id="result_confirmation",
        subject="Review the survey query result",
        body=result_data,
        response_timeout=datetime.timedelta(hours=1),
    )

    prompt_confirmation >> generate_sql >> run_query


# [END example_llm_survey_interactive]

example_llm_survey_interactive()


# ---------------------------------------------------------------------------
# DAG 2: Scheduled survey question example
# ---------------------------------------------------------------------------


# [START example_llm_survey_scheduled]
@dag(schedule="@monthly", start_date=datetime.datetime(2025, 1, 1), catchup=False)
def example_llm_survey_scheduled():
    """
    Download, validate, query, and report on the survey CSV on a schedule.

    Task graph::

        download_survey (HttpOperator)
            → prepare_csv (@task)
            → check_schema (LLMSchemaCompareOperator)
            → generate_sql (LLMSQLQueryOperator)
            → run_query (AnalyticsOperator)
            → extract_data (@task)
            → send_result (@task)

    No human review steps -- suitable for recurring reporting or dashboards.
    Change ``schedule`` to any cron expression or Airflow timetable to adjust
    the run frequency.

    Prerequisites:

    - HTTP connection ``airflow_website`` pointing at ``https://airflow.apache.org``.
    - Set ``SMTP_CONN_ID`` and ``NOTIFY_EMAIL`` environment variables to enable
      email delivery of results; otherwise results are logged to the task log.
    """
    # ------------------------------------------------------------------
    # Step 1: Download the survey CSV from the Airflow website.
    # ------------------------------------------------------------------
    download_survey = HttpOperator(
        task_id="download_survey",
        http_conn_id=AIRFLOW_WEBSITE_CONN_ID,
        endpoint=SURVEY_CSV_ENDPOINT,
        method="GET",
        response_filter=lambda r: r.text,
        log_response=False,
    )

    # ------------------------------------------------------------------
    # Step 2: Write the downloaded CSV to disk and generate a reference
    # schema file for the schema comparison step.
    # ------------------------------------------------------------------
    @task
    def prepare_csv(csv_text: str) -> None:
        os.makedirs(os.path.dirname(SURVEY_CSV_PATH), exist_ok=True)
        with open(SURVEY_CSV_PATH, "w", encoding="utf-8") as f:
            f.write(csv_text)

        # Write a single-row reference CSV from the schema context so
        # LLMSchemaCompareOperator has a structured baseline to compare against.
        os.makedirs(os.path.dirname(REFERENCE_CSV_PATH), exist_ok=True)
        columns = [line.split('"')[1] for line in SURVEY_SCHEMA.strip().splitlines() if '"' in line]
        with open(REFERENCE_CSV_PATH, "w", newline="", encoding="utf-8") as ref:
            csv_mod.writer(ref).writerow(columns)

    csv_ready = prepare_csv(download_survey.output)

    # ------------------------------------------------------------------
    # Step 3: Validate the downloaded CSV schema against the reference.
    # Raises if critical columns are missing or renamed.
    # ------------------------------------------------------------------
    check_schema = LLMSchemaCompareOperator(
        task_id="check_schema",
        prompt="""\
Compare the survey CSV schema against the reference schema.
Flag any missing or renamed columns that would break the downstream SQL queries.""",
        llm_conn_id=LLM_CONN_ID,
        data_sources=[survey_datasource, reference_datasource],
        context_strategy="basic",
    )
    csv_ready >> check_schema

    # ------------------------------------------------------------------
    # Step 4: SQL generation -- LLM translates the fixed question.
    # ------------------------------------------------------------------
    generate_sql = LLMSQLQueryOperator(
        task_id="generate_sql",
        prompt=SCHEDULED_PROMPT,
        llm_conn_id=LLM_CONN_ID,
        datasource_config=survey_datasource,
        schema_context=SURVEY_SCHEMA,
    )
    check_schema >> generate_sql

    # ------------------------------------------------------------------
    # Step 5: SQL execution via Apache DataFusion.
    # ------------------------------------------------------------------
    run_query = AnalyticsOperator(
        task_id="run_query",
        datasource_configs=[survey_datasource],
        queries=["{{ ti.xcom_pull(task_ids='generate_sql') }}"],
        result_output_format="json",
    )

    # ------------------------------------------------------------------
    # Step 6: Extract data rows from the JSON result.
    # AnalyticsOperator returns [{"query": "...", "data": [...]}, ...]
    # ------------------------------------------------------------------
    @task
    def extract_data(raw: str) -> str:
        results = json.loads(raw)
        data = [row for item in results for row in item["data"]]
        return json.dumps(data, indent=2)

    result_data = extract_data(run_query.output)

    # ------------------------------------------------------------------
    # Step 7: Send result via email if SMTP is configured, otherwise log.
    # Set the SMTP_CONN_ID and NOTIFY_EMAIL environment variables to enable
    # email delivery.
    # ------------------------------------------------------------------
    @task
    def send_result(data: str) -> None:
        if SMTP_CONN_ID and NOTIFY_EMAIL:
            from airflow.providers.smtp.hooks.smtp import SmtpHook

            with SmtpHook(smtp_conn_id=SMTP_CONN_ID) as hook:
                hook.send_email_smtp(
                    to=NOTIFY_EMAIL,
                    subject=f"Airflow Survey Analysis: {SCHEDULED_PROMPT}",
                    html_content=f"<pre>{data}</pre>",
                )
        else:
            print(f"Survey analysis result:\n{data}")

    generate_sql >> run_query >> result_data >> send_result(result_data)


# [END example_llm_survey_scheduled]

example_llm_survey_scheduled()
