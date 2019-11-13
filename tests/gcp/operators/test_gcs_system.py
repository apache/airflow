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

from airflow.gcp.example_dags.example_gcs import (
    BUCKET_1, BUCKET_2, PATH_TO_SAVED_FILE, PATH_TO_TRANSFORM_SCRIPT, PATH_TO_UPLOAD_FILE,
)
from tests.gcp.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

SCRIPT = """import sys
source = sys.argv[1]
destination = sys.argv[2]

print('running script')
with open(source, "r") as src, open(destination, "w+") as dest:
    lines = [l.upper() for l in src.readlines()]
    print(lines)
    dest.writelines(lines)
    """

command = GcpSystemTest.commands_registry()


@command
def create_files():
    GcpSystemTest.create_temp_file(
        dir_path="", filename=PATH_TO_UPLOAD_FILE, content="This is a test file"
    )
    GcpSystemTest.create_temp_file(
        dir_path="", filename=PATH_TO_TRANSFORM_SCRIPT, content=SCRIPT
    )


@command
def delete_files():
    GcpSystemTest.delete_temp_file(filename=PATH_TO_UPLOAD_FILE)
    GcpSystemTest.delete_temp_file(filename=PATH_TO_TRANSFORM_SCRIPT)
    GcpSystemTest.delete_temp_file(filename=PATH_TO_SAVED_FILE)


@command
def remove_buckets():
    GcpSystemTest.delete_gcs_bucket(BUCKET_1)
    GcpSystemTest.delete_gcs_bucket(BUCKET_2)


@pytest.fixture()
def helper():
    create_files()
    yield
    delete_files()
    remove_buckets()


@command
@GcpSystemTest.skip(GCP_GCS_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    with provide_gcp_context(GCP_GCS_KEY):
        GcpSystemTest.run_dag("example_gcs", GCP_DAG_FOLDER)
