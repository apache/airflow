# -*- coding: utf-8 -*-
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

import pytest

from airflow.gcp.example_dags.example_bigquery_dts import (
    BUCKET_URI, GCP_DTS_BQ_DATASET, GCP_DTS_BQ_TABLE, GCP_PROJECT_ID,
)
from tests.gcp.utils.gcp_authenticator import GCP_BIGQUERY_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

command = GcpSystemTest.commands_registry()

DATASET_NAME = f"{GCP_PROJECT_ID}:{GCP_DTS_BQ_DATASET}"
TABLE_NAME = f"{GCP_DTS_BQ_DATASET}.{GCP_DTS_BQ_TABLE}"


@command
def create_dataset():
    GcpSystemTest.execute_with_ctx(
        cmd=["bq", "--location", "us", "mk", "--dataset", DATASET_NAME],
        key=GCP_BIGQUERY_KEY,
    )
    table_name = f"{DATASET_NAME}.{GCP_DTS_BQ_TABLE}"
    GcpSystemTest.execute_with_ctx(
        cmd=["bq", "mk", "--table", table_name], key=GCP_BIGQUERY_KEY
    )


@command
def upload_data():
    GcpSystemTest.execute_with_ctx(
        cmd=[
            "bq",
            "--location",
            "us",
            "load",
            "--autodetect",
            "--source_format",
            "CSV",
            TABLE_NAME,
            BUCKET_URI,
        ],
        key=GCP_BIGQUERY_KEY,
    )


@command
def delete_dataset():
    GcpSystemTest.execute_with_ctx(
        cmd=["bq", "rm", "-r", "-f", "-d", DATASET_NAME], key=GCP_BIGQUERY_KEY
    )


@pytest.fixture
def helper():
    create_dataset()
    upload_data()
    yield
    delete_dataset()


@command
@GcpSystemTest.skip(GCP_BIGQUERY_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag_function():
    with provide_gcp_context(GCP_BIGQUERY_KEY):
        GcpSystemTest.run_dag("example_gcp_bigquery_dts", GCP_DAG_FOLDER)
