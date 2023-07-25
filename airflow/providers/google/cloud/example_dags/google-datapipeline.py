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

from airflow import models
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.google.cloud.operators.datapipeline import CreateDataPipelineOperator, RunDataPipelineOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "google-datapipeline"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "datapipeline"],
) as dag:
    run_data_pipeline = RunDataPipelineOperator(
        task_id = "run_data_pipeline",
        data_pipeline_name = "dp-create-1642676351302-mp--1675461000"
        )
    run_data_pipeline
