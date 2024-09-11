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
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Java.

Important Note:
    This test downloads Java JAR file from the public bucket. In case the JAR file cannot be downloaded
    or is not compatible with the Java version used in the test, the source code for this test can be
    downloaded from here (https://beam.apache.org/get-started/wordcount-example) and needs to be compiled
    manually in order to work.

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
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataflow_native_java"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
PUBLIC_BUCKET = "airflow-system-tests-resources"

JAR_FILE_NAME = "word-count-beam-bundled-0.1.jar"
# For the distributed system, we need to store the JAR file in a folder that can be accessed by multiple
# worker.
# For example in Composer the correct path is gcs/data/word-count-beam-bundled-0.1.jar.
# Because gcs/data/ is shared folder for Airflow's workers.
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
LOCAL_JAR = f"gcs/data/{JAR_FILE_NAME}" if IS_COMPOSER else JAR_FILE_NAME
REMOTE_JAR_FILE_PATH = f"dataflow/java/{JAR_FILE_NAME}"
GCS_JAR = f"gs://{PUBLIC_BUCKET}/dataflow/java/{JAR_FILE_NAME}"
GCS_OUTPUT = f"gs://{BUCKET_NAME}"
LOCATION = "europe-west3"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow", "java"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=REMOTE_JAR_FILE_PATH,
        bucket=PUBLIC_BUCKET,
        filename=LOCAL_JAR,
    )

    # [START howto_operator_start_java_job_local_jar]
    start_java_job_local = BeamRunJavaPipelineOperator(
        task_id="start_java_job_local",
        jar=LOCAL_JAR,
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={
            "check_if_running": CheckJobRunning.WaitForRun,
            "location": LOCATION,
            "poll_sleep": 10,
        },
    )
    # [END howto_operator_start_java_job_local_jar]

    # [START howto_operator_start_java_job_jar_on_gcs]
    start_java_job = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_java_job",
        jar=GCS_JAR,
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={
            "job_name": "test-java-pipeline-job",
            "check_if_running": CheckJobRunning.IgnoreJob,
            "location": LOCATION,
            "poll_sleep": 10,
            "append_job_name": False,
        },
    )
    # [END howto_operator_start_java_job_jar_on_gcs]

    # [START howto_operator_start_java_job_jar_on_gcs_deferrable]
    start_java_deferrable = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_java_job_deferrable",
        jar=GCS_JAR,
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={
            "job_name": "test-deferrable-java-pipeline-job",
            "check_if_running": CheckJobRunning.WaitForRun,
            "location": LOCATION,
            "poll_sleep": 10,
            "append_job_name": False,
        },
        deferrable=True,
    )
    # [END howto_operator_start_java_job_jar_on_gcs_deferrable]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> download_file
        # TEST BODY
        >> [start_java_job_local, start_java_job, start_java_deferrable]
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
