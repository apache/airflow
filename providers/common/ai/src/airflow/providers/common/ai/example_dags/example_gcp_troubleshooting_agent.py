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
Example Dag: troubleshoot a Google Cloud data pipeline with GoogleCloudToolset.

Required Airflow Variables:
- ``common_ai_gcp_project_id``
- ``common_ai_gcp_bucket``

Optional Airflow Variables:
- ``common_ai_gcp_dataset``; default: ``pipeline_observability``
- ``common_ai_gcp_pipeline_table``; default: ``pipeline_runs``
- ``common_ai_gcp_raw_prefix``; default: ``raw/``

For a useful demo, point the variables at a project with:
- GCS objects under ``gs://<bucket>/<raw_prefix>``
- a BigQuery table with recent pipeline status rows
- Cloud Monitoring metrics from recent BigQuery and GCS activity
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.google import GoogleCloudToolset
from airflow.providers.common.compat.sdk import Variable, dag, task


# [START howto_operator_agent_gcp_troubleshooting]
@dag(tags=["example"])
def example_gcp_troubleshooting_agent():
    @task
    def build_triage_prompt() -> str:
        project_id = Variable.get("common_ai_gcp_project_id")
        bucket_name = Variable.get("common_ai_gcp_bucket")
        dataset_id = Variable.get("common_ai_gcp_dataset", default="pipeline_observability")
        pipeline_table = Variable.get("common_ai_gcp_pipeline_table", default="pipeline_runs")
        raw_prefix = Variable.get("common_ai_gcp_raw_prefix", default="raw/")

        return (
            f"Pipeline is slow today in project {project_id}. "
            f"Check whether raw files arrived in gs://{bucket_name}/{raw_prefix}, "
            "inspect recent BigQuery jobs and rows from "
            f"{dataset_id}.{pipeline_table}, check active Cloud Monitoring alert policies, "
            "and compare BigQuery query count and GCS request count metrics for today versus yesterday. "
            "Summarize the likely cause and the next action."
        )

    AgentOperator(
        task_id="triage_pipeline",
        prompt=build_triage_prompt(),
        llm_conn_id="pydanticai_default",
        system_prompt="Use the available Google tools to investigate the pipeline. Do not invent missing values.",
        toolsets=[
            GoogleCloudToolset(
                gcp_conn_id="google_cloud_default",
                allowed_methods=[
                    "storage/v1:objects.list",
                    "bigquery/v2:jobs.list",
                    "bigquery/v2:jobs.get",
                    "bigquery/v2:jobs.query",
                    "monitoring/v3:projects.alertPolicies.list",
                    "monitoring/v3:projects.timeSeries.list",
                ],
                max_pages=2,
                max_output_bytes=20000,
            )
        ],
    )


# [END howto_operator_agent_gcp_troubleshooting]

example_gcp_troubleshooting_agent()
