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
"""Example DAG: triage support tickets with @task.llm, structured output, and dynamic task mapping."""

from __future__ import annotations

from pydantic import BaseModel

from airflow.providers.common.compat.sdk import dag, task


# [START howto_decorator_llm_pipeline]
@dag
def example_llm_analysis_pipeline():
    class TicketAnalysis(BaseModel):
        priority: str
        category: str
        summary: str
        suggested_action: str

    @task
    def get_support_tickets():
        """Fetch unprocessed support tickets."""
        return [
            (
                "Our nightly ETL pipeline has been failing for the past 3 days. "
                "The error shows a connection timeout to the Postgres source database. "
                "This is blocking our daily financial reports."
            ),
            (
                "We'd like to add a new connection type for our internal ML model registry. "
                "Is there documentation on creating custom hooks?"
            ),
            (
                "After upgrading to the latest version, the Grid view takes over "
                "30 seconds to load for DAGs with more than 500 tasks. "
                "Previously it loaded in under 5 seconds."
            ),
        ]

    @task.llm(
        llm_conn_id="pydantic_ai_default",
        system_prompt=(
            "Analyze the support ticket and extract: "
            "priority (critical/high/medium/low), "
            "category (bug/feature_request/question/performance), "
            "a one-sentence summary, and a suggested next action."
        ),
        output_type=TicketAnalysis,
    )
    def analyze_ticket(ticket: str):
        return f"Analyze this support ticket:\n\n{ticket}"

    @task
    def store_results(analyses: list[dict]):
        """Store ticket analyses. In production, this would write to a database or ticketing system."""
        for analysis in analyses:
            print(f"[{analysis['priority'].upper()}] {analysis['category']}: {analysis['summary']}")

    tickets = get_support_tickets()
    analyses = analyze_ticket.expand(ticket=tickets)
    store_results(analyses)


# [END howto_decorator_llm_pipeline]

example_llm_analysis_pipeline()
