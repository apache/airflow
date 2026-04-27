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
"""Example DAGs demonstrating LLMFileAnalysisOperator usage."""

from __future__ import annotations

from pydantic import BaseModel

from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_llm_file_analysis_basic]
@dag
def example_llm_file_analysis_basic():
    LLMFileAnalysisOperator(
        task_id="analyze_error_logs",
        prompt="Find error patterns and correlate them with deployment timestamps.",
        llm_conn_id="pydanticai_default",
        file_path="s3://logs/app/2024-01-15/",
        file_conn_id="aws_default",
    )


# [END howto_operator_llm_file_analysis_basic]

example_llm_file_analysis_basic()


# [START howto_operator_llm_file_analysis_prefix]
@dag
def example_llm_file_analysis_prefix():
    LLMFileAnalysisOperator(
        task_id="summarize_partitioned_logs",
        prompt=(
            "Summarize recurring errors across these partitioned log files and call out "
            "which partition keys appear in the highest-severity findings."
        ),
        llm_conn_id="pydanticai_default",
        file_path="s3://logs/app/dt=2024-01-15/",
        file_conn_id="aws_default",
        max_files=10,
        max_total_size_bytes=10 * 1024 * 1024,
        max_text_chars=20_000,
    )


# [END howto_operator_llm_file_analysis_prefix]

example_llm_file_analysis_prefix()


# [START howto_operator_llm_file_analysis_multimodal]
@dag
def example_llm_file_analysis_multimodal():
    LLMFileAnalysisOperator(
        task_id="validate_dashboards",
        prompt="Check charts for visual anomalies or stale data indicators.",
        llm_conn_id="pydanticai_default",
        file_path="s3://monitoring/dashboards/latest.png",
        file_conn_id="aws_default",
        multi_modal=True,
    )


# [END howto_operator_llm_file_analysis_multimodal]

example_llm_file_analysis_multimodal()


# [START howto_operator_llm_file_analysis_structured]
@dag
def example_llm_file_analysis_structured():

    class FileAnalysisSummary(BaseModel):
        """Structured output schema for the file-analysis examples."""

        findings: list[str]
        highest_severity: str
        truncated_inputs: bool

    LLMFileAnalysisOperator(
        task_id="analyze_parquet_quality",
        prompt=(
            "Return the top data-quality findings from this Parquet dataset. "
            "Include whether any inputs were truncated."
        ),
        llm_conn_id="pydanticai_default",
        file_path="s3://analytics/warehouse/customers/",
        file_conn_id="aws_default",
        output_type=FileAnalysisSummary,
        sample_rows=5,
        max_files=5,
    )


# [END howto_operator_llm_file_analysis_structured]

example_llm_file_analysis_structured()


# [START howto_decorator_llm_file_analysis]
@dag
def example_llm_file_analysis_decorator():
    @task.llm_file_analysis(
        llm_conn_id="pydanticai_default",
        file_path="s3://analytics/reports/quarterly.pdf",
        file_conn_id="aws_default",
        multi_modal=True,
    )
    def review_quarterly_report():
        return "Extract the key revenue, risk, and compliance findings from this report."

    review_quarterly_report()


# [END howto_decorator_llm_file_analysis]

example_llm_file_analysis_decorator()
