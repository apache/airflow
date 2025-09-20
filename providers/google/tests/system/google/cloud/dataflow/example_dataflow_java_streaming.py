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
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Java(streaming).

Important Note:
    This test downloads Java JAR file from the public bucket. In case the JAR file cannot be downloaded
    or is not compatible with the Java version used in the test.
    There is no streaming pipeline example for Apache Beam Java SDK, the source code and build instructions
    are located in `providers/google/tests/system/google/cloud/dataflow/resources/java_streaming_src/`.

    You can follow the instructions on how to pack a self-executing jar here:
    https://beam.apache.org/documentation/runners/dataflow/

Requirements:
    These operators require the gcloud command and Java's JRE to run.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubDeleteTopicOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "dataflow_java_streaming"
LOCATION = "europe-west3"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"
GCS_TMP = f"gs://{BUCKET_NAME}/temp"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/DF_OUT"
RESOURCE_BUCKET = "airflow-system-tests-resources"
JAR_FILE_NAME = "stream-pubsub-example-bundled-v-0.1.jar"
GCS_JAR_PATH = f"gs://{RESOURCE_BUCKET}/dataflow/java/{JAR_FILE_NAME}"
# For the distributed system, we need to store the JAR file in a folder that can be accessed by multiple
# worker.
# For example in Composer the correct path is gcs/data/word-count-beam-bundled-0.1.jar.
# Because gcs/data/ is shared folder for Airflow's workers.
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
LOCAL_JAR = f"gcs/data/{JAR_FILE_NAME}" if IS_COMPOSER else JAR_FILE_NAME
REMOTE_JAR_FILE_PATH = f"dataflow/java/{JAR_FILE_NAME}"

OUTPUT_TOPIC_ID = f"tp-{ENV_ID}-out"
OUTPUT_TOPIC_ID_2 = f"tp-2-{ENV_ID}-out"
INPUT_TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime"
OUTPUT_TOPIC = f"projects/{PROJECT_ID}/topics/{OUTPUT_TOPIC_ID}"
OUTPUT_TOPIC_2 = f"projects/{PROJECT_ID}/topics/{OUTPUT_TOPIC_ID_2}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["example", "dataflow", "java", "streaming"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=f"dataflow/java/{JAR_FILE_NAME}",
        bucket=RESOURCE_BUCKET,
        filename=LOCAL_JAR,
    )
    create_output_pub_sub_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=OUTPUT_TOPIC_ID, project_id=PROJECT_ID, fail_if_exists=False
    )
    create_output_pub_sub_topic_2 = PubSubCreateTopicOperator(
        task_id="create_topic_2", topic=OUTPUT_TOPIC_ID_2, project_id=PROJECT_ID, fail_if_exists=False
    )
    # [START howto_operator_start_java_streaming]
    start_java_streaming_job_dataflow = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_java_streaming_dataflow_job",
        jar=LOCAL_JAR,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "input_topic": INPUT_TOPIC,
            "output_topic": OUTPUT_TOPIC,
            "streaming": True,
        },
        dataflow_config={
            "job_name": f"java-streaming-job-{ENV_ID}",
            "location": LOCATION,
        },
    )
    # [END howto_operator_start_java_streaming]

    # [START howto_operator_start_java_streaming_deferrable]
    start_java_streaming_job_dataflow_def = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_java_streaming_dataflow_job_def",
        jar=LOCAL_JAR,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "input_topic": INPUT_TOPIC,
            "output_topic": OUTPUT_TOPIC_2,
            "streaming": True,
        },
        dataflow_config={
            "job_name": f"java-streaming-job-def-{ENV_ID}",
            "location": LOCATION,
        },
        deferrable=True,
    )
    # [END howto_operator_start_java_streaming_deferrable]

    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=LOCATION,
        job_id="{{ task_instance.xcom_pull(task_ids='start_java_streaming_dataflow_job')['dataflow_job_id'] }}",
    )
    stop_dataflow_job_deferrable = DataflowStopJobOperator(
        task_id="stop_dataflow_job_deferrable",
        location=LOCATION,
        job_id="{{ task_instance.xcom_pull(task_ids='start_java_streaming_dataflow_job_def', key='dataflow_job_id') }}",
    )
    delete_topic = PubSubDeleteTopicOperator(
        task_id="delete_topic", topic=OUTPUT_TOPIC_ID, project_id=PROJECT_ID
    )
    delete_topic.trigger_rule = TriggerRule.ALL_DONE

    delete_topic_2 = PubSubDeleteTopicOperator(
        task_id="delete_topic_2", topic=OUTPUT_TOPIC_ID_2, project_id=PROJECT_ID
    )
    delete_topic_2.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> download_file
        >> [create_output_pub_sub_topic, create_output_pub_sub_topic_2]
        # TEST BODY
        >> start_java_streaming_job_dataflow
        >> stop_dataflow_job
        >> start_java_streaming_job_dataflow_def
        >> stop_dataflow_job_deferrable
        # TEST TEARDOWN
        >> delete_topic
        >> delete_topic_2
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
