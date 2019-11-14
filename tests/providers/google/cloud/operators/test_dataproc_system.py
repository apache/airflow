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

from tests.gcp.utils.gcp_authenticator import GCP_DATAPROC_KEY
from tests.providers.google.cloud.operators.test_dataproc_operator_system_helper import DataprocTestHelper
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)


@skip_gcp_system(GCP_DATAPROC_KEY, require_local_executor=True)
class DataprocExampleDagsTest(SystemTest):
    helper = DataprocTestHelper()

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def setUp(self):
        super().setUp()
        self.helper.create_test_bucket(BUCKET)
        self.helper.upload_test_file(PYSPARK_URI, PYSPARK_MAIN)

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def tearDown(self):
        self.helper.delete_gcs_bucket_elements(BUCKET)
        super().tearDown()

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def test_run_example_dag(self):
        self.run_dag(dag_id="example_gcp_dataproc", dag_folder=GCP_DAG_FOLDER)
