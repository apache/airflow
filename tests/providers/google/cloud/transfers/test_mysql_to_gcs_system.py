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
from psycopg2 import ProgrammingError, OperationalError

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.example_dags.example_mysql_to_gcs import GCS_BUCKET
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

CREATE_QUERY = """
CREATE TABLE test_table
(
    id int auto_increment primary key,
    params json
);
"""

LOAD_QUERY = """
INSERT INTO test_table (id, params)
VALUES
   (
      1, '{ "customer": "Lily Bush", "items": {"product": "Diaper","qty": 24}}'
   ),
   (
      2, '{ "customer": "Josh William", "items": {"product": "Toy Car","qty": 1}}'
   ),
   (
      3, '{ "customer": "Mary Clark", "items": {"product": "Toy Train","qty": 2}}'
   );
"""
DELETE_QUERY = "DROP TABLE test_table;"


@pytest.mark.backend("mysql")
@pytest.mark.credential_file(GCP_GCS_KEY)
class MySQLToGCSSystemTest(GoogleSystemTest):
    @staticmethod
    def init_db():
        try:
            hook = MySqlHook()
            hook.run(CREATE_QUERY)
            hook.run(LOAD_QUERY)
        except (OperationalError, ProgrammingError):
            pass

    @staticmethod
    def drop_db():
        hook = MySqlHook()
        hook.run(DELETE_QUERY)

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(GCS_BUCKET)
        self.init_db()

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_mysql_to_gcs', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(GCS_BUCKET)
        self.drop_db()
        super().tearDown()
