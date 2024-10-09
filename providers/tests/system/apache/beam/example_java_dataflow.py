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
Example Airflow DAG for Apache Beam operators
"""

from __future__ import annotations

from airflow import models
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

from providers.tests.system.apache.beam.utils import (
    GCS_JAR_DATAFLOW_RUNNER_BUCKET_NAME,
    GCS_JAR_DATAFLOW_RUNNER_OBJECT_NAME,
    GCS_OUTPUT,
    GCS_STAGING,
    GCS_TMP,
    START_DATE,
)

with models.DAG(
    "example_beam_native_java_dataflow_runner",
    schedule=None,  # Override to match your needs
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_start_java_dataflow_runner_pipeline]
    jar_to_local_dataflow_runner = GCSToLocalFilesystemOperator(
        task_id="jar_to_local_dataflow_runner",
        bucket=GCS_JAR_DATAFLOW_RUNNER_BUCKET_NAME,
        object_name=GCS_JAR_DATAFLOW_RUNNER_OBJECT_NAME,
        filename="/tmp/beam_wordcount_dataflow_runner_{{ ds_nodash }}.jar",
    )

    start_java_pipeline_dataflow = BeamRunJavaPipelineOperator(
        task_id="start_java_pipeline_dataflow",
        runner="DataflowRunner",
        jar="/tmp/beam_wordcount_dataflow_runner_{{ ds_nodash }}.jar",
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "output": GCS_OUTPUT,
        },
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={"job_name": "{{task.task_id}}", "location": "us-central1"},
    )

    jar_to_local_dataflow_runner >> start_java_pipeline_dataflow
    # [END howto_operator_start_java_dataflow_runner_pipeline]


from dev.tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
