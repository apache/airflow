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
"""System tests for Google Cloud Build operators"""

import os

import pytest

from airflow.providers.google.cloud.example_dags.example_sftp_to_gcs import (
    BUCKET_SRC, DIR, OBJECT_SRC_1, OBJECT_SRC_2, OBJECT_SRC_3, SUBDIR, TMP_PATH,
)
from tests.gcp.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GcpSystemTest, provide_gcp_context

command = GcpSystemTest.commands_registry()


FILES_AND_DIRS = [
    (OBJECT_SRC_1, os.path.join(TMP_PATH, DIR)),
    (OBJECT_SRC_2, os.path.join(TMP_PATH, DIR)),
    (OBJECT_SRC_3, os.path.join(TMP_PATH, DIR)),
    (OBJECT_SRC_1, os.path.join(TMP_PATH, DIR, SUBDIR)),
    (OBJECT_SRC_2, os.path.join(TMP_PATH, DIR, SUBDIR)),
    (OBJECT_SRC_3, os.path.join(TMP_PATH, DIR, SUBDIR)),
]


@command
def create_bucket():
    GcpSystemTest.create_gcs_bucket(BUCKET_SRC)


@command
def create_temp_files():
    for filename, dir_path in FILES_AND_DIRS:
        GcpSystemTest.create_temp_file(filename, dir_path)


@command
def delete_temp_files():
    for filename, dir_path in FILES_AND_DIRS:
        GcpSystemTest.delete_temp_file(filename, dir_path)
        GcpSystemTest.delete_temp_dir(dir_path)


@command
def delete_bucket():
    GcpSystemTest.delete_gcs_bucket(BUCKET_SRC)


@pytest.fixture
def helper():
    create_bucket()
    create_temp_files()
    yield
    delete_bucket()
    delete_temp_files()


@command
@GcpSystemTest.skip(GCP_GCS_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    """
    System tests for SFTP to Google Cloud Storage transfer operator
    It use a real service.
    """
    with provide_gcp_context(GCP_GCS_KEY):
        GcpSystemTest.run_dag("example_sftp_to_gcs", CLOUD_DAG_FOLDER)


if __name__ == "__main__":
    GcpSystemTest.cli()
