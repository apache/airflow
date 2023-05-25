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
from airflow.providers.apache.beam.operators.beam import BeamRunGoPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from tests.system.providers.apache.beam.utils import (
    DEFAULT_ARGS,
    GCP_PROJECT_ID,
    GCS_GO,
    GCS_OUTPUT,
    GCS_STAGING,
    GCS_TMP,
    START_DATE,
)

with models.DAG(
    "example_beam_native_go",
    start_date=START_DATE,
    schedule="@once",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example"],
) as dag:

    # [START howto_operator_start_go_direct_runner_pipeline_local_file]
    start_go_pipeline_local_direct_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_local_direct_runner",
        go_file="files/apache_beam/examples/wordcount.go",
    )
    # [END howto_operator_start_go_direct_runner_pipeline_local_file]

    # [START howto_operator_start_go_direct_runner_pipeline_gcs_file]
    start_go_pipeline_direct_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_direct_runner",
        go_file=GCS_GO,
        pipeline_options={"output": GCS_OUTPUT},
    )
    # [END howto_operator_start_go_direct_runner_pipeline_gcs_file]

    # [START howto_operator_start_go_dataflow_runner_pipeline_gcs_file]
    start_go_pipeline_dataflow_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_dataflow_runner",
        runner="DataflowRunner",
        go_file=GCS_GO,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "output": GCS_OUTPUT,
            "WorkerHarnessContainerImage": "apache/beam_go_sdk:latest",
        },
        dataflow_config=DataflowConfiguration(
            job_name="{{task.task_id}}", project_id=GCP_PROJECT_ID, location="us-central1"
        ),
    )
    # [END howto_operator_start_go_dataflow_runner_pipeline_gcs_file]

    start_go_pipeline_local_spark_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_local_spark_runner",
        go_file="/files/apache_beam/examples/wordcount.go",
        runner="SparkRunner",
        pipeline_options={
            "endpoint": "/your/spark/endpoint",
        },
    )

    start_go_pipeline_local_flink_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_local_flink_runner",
        go_file="/files/apache_beam/examples/wordcount.go",
        runner="FlinkRunner",
        pipeline_options={
            "output": "/tmp/start_go_pipeline_local_flink_runner",
        },
    )

    (
        [
            start_go_pipeline_local_direct_runner,
            start_go_pipeline_direct_runner,
        ]
        >> start_go_pipeline_local_flink_runner
        >> start_go_pipeline_local_spark_runner
    )


from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
