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
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Python for Streaming job.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubDeleteTopicOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "dataflow_native_python_streaming"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_PYTHON_SCRIPT = f"gs://{RESOURCE_DATA_BUCKET}/dataflow/python/streaming_wordcount.py"
LOCATION = "europe-west3"
TOPIC_ID = f"topic-{DAG_ID}"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    create_pub_sub_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=TOPIC_ID, project_id=PROJECT_ID, fail_if_exists=False
    )

    # [START howto_operator_start_streaming_python_job]
    start_streaming_python_job = BeamRunPythonPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_streaming_python_job",
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={
            "temp_location": GCS_TMP,
            "input_topic": "projects/pubsub-public-data/topics/taxirides-realtime",
            "output_topic": f"projects/{PROJECT_ID}/topics/{TOPIC_ID}",
            "streaming": True,
        },
        py_requirements=["apache-beam[gcp]==2.47.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={"location": LOCATION, "job_name": "start_python_job_streaming"},
    )
    # [END howto_operator_start_streaming_python_job]

    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=LOCATION,
        job_id="{{ task_instance.xcom_pull(task_ids='start_streaming_python_job')['dataflow_job_id'] }}",
    )

    delete_topic = PubSubDeleteTopicOperator(task_id="delete_topic", topic=TOPIC_ID, project_id=PROJECT_ID)
    delete_topic.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> create_pub_sub_topic
        # TEST BODY
        >> start_streaming_python_job
        # TEST TEARDOWN
        >> stop_dataflow_job
        >> delete_topic
        >> delete_bucket
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
