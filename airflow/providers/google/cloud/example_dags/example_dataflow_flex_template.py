#
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
Example Airflow DAG for Google Cloud Dataflow service
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

DATAFLOW_FLEX_TEMPLATE_JOB_NAME = os.environ.get(
    "GCP_DATAFLOW_FLEX_TEMPLATE_JOB_NAME", "dataflow-flex-template"
)

# For simplicity we use the same topic name as the subscription name.
PUBSUB_FLEX_TEMPLATE_TOPIC = os.environ.get(
    "GCP_DATAFLOW_PUBSUB_FLEX_TEMPLATE_TOPIC", "dataflow-flex-template"
)
PUBSUB_FLEX_TEMPLATE_SUBSCRIPTION = PUBSUB_FLEX_TEMPLATE_TOPIC
GCS_FLEX_TEMPLATE_TEMPLATE_PATH = os.environ.get(
    "GCP_DATAFLOW_GCS_FLEX_TEMPLATE_TEMPLATE_PATH",
    "gs://INVALID BUCKET NAME/samples/dataflow/templates/streaming-beam-sql.json",
)
BQ_FLEX_TEMPLATE_DATASET = os.environ.get("GCP_DATAFLOW_BQ_FLEX_TEMPLATE_DATASET", "airflow_dataflow_samples")
BQ_FLEX_TEMPLATE_LOCATION = os.environ.get("GCP_DATAFLOW_BQ_FLEX_TEMPLATE_LOCATION>", "us-west1")

with models.DAG(
    dag_id="example_gcp_dataflow_flex_template_java",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag_flex_template:
    # [START howto_operator_start_template_job]
    start_flex_template = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_streaming_beam_sql",
        body={
            "launchParameter": {
                "containerSpecGcsPath": GCS_FLEX_TEMPLATE_TEMPLATE_PATH,
                "jobName": DATAFLOW_FLEX_TEMPLATE_JOB_NAME,
                "parameters": {
                    "inputSubscription": PUBSUB_FLEX_TEMPLATE_SUBSCRIPTION,
                    "outputTable": f"{GCP_PROJECT_ID}:{BQ_FLEX_TEMPLATE_DATASET}.streaming_beam_sql",
                },
            }
        },
        do_xcom_push=True,
        location=BQ_FLEX_TEMPLATE_LOCATION,
    )
    # [END howto_operator_start_template_job]
