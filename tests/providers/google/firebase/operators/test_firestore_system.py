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

from airflow.providers.google.firebase.example_dags.example_firestore import (
    DATASET_NAME,
    EXPORT_DESTINATION_URL,
)
from tests.providers.google.cloud.utils.gcp_authenticator import G_FIREBASE_KEY
from tests.test_utils.gcp_system_helpers import FIREBASE_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.system("google.firebase")
@pytest.mark.system("google.cloud")
@pytest.mark.credential_file(G_FIREBASE_KEY)
class CloudFirestoreSystemTest(GoogleSystemTest):
    def setUp(self):
        super().setUp()
        self.clean_up()

    def tearDown(self):
        self.clean_up()
        super().tearDown()

    def clean_up(self):
        self.execute_with_ctx(["gsutil", "rm", "-r", f"{EXPORT_DESTINATION_URL}"], G_FIREBASE_KEY)
        self.execute_with_ctx(
            ["bq", "rm", '--force', '--recursive', f"{self._project_id()}:{DATASET_NAME}"], G_FIREBASE_KEY
        )

    @provide_gcp_context(G_FIREBASE_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_google_firestore', FIREBASE_DAG_FOLDER)
