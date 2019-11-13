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
from urllib.parse import urlparse

import pytest

from airflow.gcp.example_dags.example_mlengine import (
    JOB_DIR, PREDICTION_OUTPUT, SAVED_MODEL_PATH, SUMMARY_STAGING, SUMMARY_TMP,
)
from tests.gcp.utils.gcp_authenticator import GCP_AI_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

command = GcpSystemTest.commands_registry()


def _get_ephemeral_bucket_names():
    return {
        urlparse(bucket_url).netloc
        for bucket_url in {
            SAVED_MODEL_PATH,
            JOB_DIR,
            PREDICTION_OUTPUT,
            SUMMARY_TMP,
            SUMMARY_STAGING,
        }
    }


@command
def create_buckets():
    for bucket in _get_ephemeral_bucket_names():
        GcpSystemTest.create_gcs_bucket(bucket)


@command
def delete_buckets():
    for bucket in _get_ephemeral_bucket_names():
        GcpSystemTest.delete_gcs_bucket(bucket)


@pytest.fixture
def helper():
    create_buckets()
    yield
    delete_buckets()


@command
@GcpSystemTest.skip(GCP_AI_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    with provide_gcp_context(GCP_AI_KEY):
        GcpSystemTest.run_dag("example_gcp_mlengine", GCP_DAG_FOLDER)
