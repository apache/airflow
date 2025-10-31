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
Example Airflow DAG for testing Google Dataflow to create pipelines.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePipelineOperator,
    DataflowDeletePipelineOperator,
    DataflowStopJobOperator,
)
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "dataflow_pipeline_streaming"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
GCP_LOCATION = "europe-central2"

PIPELINE_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")
PIPELINE_JOB_NAME = f"{DAG_ID}-job".replace("_", "-")

PIPELINE_TYPE = "PIPELINE_TYPE_STREAMING"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("-", "_")

INPUT_TOPIC_ID = "taxirides-realtime"
INPUT_TOPIC_PROJECT_ID = "pubsub-public-data"
INPUT_TOPIC = f"projects/{INPUT_TOPIC_PROJECT_ID}/topics/{INPUT_TOPIC_ID}"
SUBSCRIPTION_NAME = "taxirides-sub"

OUTPUT_TOPIC_ID = f"df-tp-out-{ENV_ID}"
OUTPUT_TOPIC = f"projects/{GCP_PROJECT_ID}/topics/{OUTPUT_TOPIC_ID}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "dataflow", "pipelines", "streaming"],
) as dag:
    create_output_pub_sub_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=OUTPUT_TOPIC_ID, project_id=GCP_PROJECT_ID, fail_if_exists=False
    )
    create_subscription = PubSubCreateSubscriptionOperator(
        task_id="create_subscription",
        project_id=INPUT_TOPIC_PROJECT_ID,
        subscription_project_id=GCP_PROJECT_ID,
        topic=INPUT_TOPIC_ID,
        subscription=SUBSCRIPTION_NAME,
    )

    # [START howto_operator_create_dataflow_pipeline_streaming]
    create_pipeline = DataflowCreatePipelineOperator(
        task_id="create_pipeline",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body={
            "name": f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/pipelines/{PIPELINE_NAME}",
            "type": PIPELINE_TYPE,
            "workload": {
                "dataflowLaunchTemplateRequest": {
                    "projectId": GCP_PROJECT_ID,
                    "location": GCP_LOCATION,
                    "gcsPath": "gs://dataflow-templates-europe-central2/latest/Cloud_PubSub_to_Cloud_PubSub",
                    "launchParameters": {
                        "jobName": PIPELINE_JOB_NAME,
                        "parameters": {
                            "inputSubscription": f"projects/{GCP_PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}",
                            "outputTopic": OUTPUT_TOPIC,
                        },
                    },
                }
            },
        },
    )
    # [END howto_operator_create_dataflow_pipeline_streaming]

    delete_pipeline = DataflowDeletePipelineOperator(
        task_id="delete_pipeline",
        pipeline_name=PIPELINE_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_output_pub_sub_topic = PubSubDeleteTopicOperator(
        task_id="delete_out_topic",
        topic=OUTPUT_TOPIC_ID,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_subscription = PubSubDeleteSubscriptionOperator(
        task_id="delete_subscription",
        project_id=GCP_PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=GCP_LOCATION,
        job_name_prefix=PIPELINE_NAME[:20],
        drain_pipeline=False,
        retries=3,
        retry_delay=timedelta(seconds=45),
    )

    (
        # TEST SETUP
        [create_output_pub_sub_topic, create_subscription]
        # TEST BODY
        >> create_pipeline
        # TEST TEARDOWN
        >> [delete_output_pub_sub_topic, delete_subscription]
        >> delete_pipeline
        >> stop_dataflow_job
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
