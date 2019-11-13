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
import os

import pytest

from tests.gcp.utils.gcp_authenticator import GCP_DATAPROC_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GcpSystemTest, provide_gcp_context

BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-test")
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)

command = GcpSystemTest.commands_registry()


@command
def create_bucket():
    GcpSystemTest.create_gcs_bucket(BUCKET)


@command
def delete_bucket():
    GcpSystemTest.delete_gcs_bucket(BUCKET)


@command
def upload_file():
    content = [
        "#!/usr/bin/python\n",
        "import pyspark\n",
        "sc = pyspark.SparkContext()\n",
        "rdd = sc.parallelize(['Hello,', 'world!'])\n",
        "words = sorted(rdd.collect())\n",
        "print(words)\n",
    ]
    GcpSystemTest.upload_content_to_gcs(content, PYSPARK_URI, PYSPARK_MAIN)


@pytest.fixture
def helper():
    create_bucket()
    upload_file()
    yield
    delete_bucket()


@command
@GcpSystemTest.skip(GCP_DATAPROC_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    with provide_gcp_context(GCP_DATAPROC_KEY):
        GcpSystemTest.run_dag(
            dag_id="example_gcp_dataproc", dag_folder=CLOUD_DAG_FOLDER
        )


if __name__ == "__main__":
    GcpSystemTest.cli()
