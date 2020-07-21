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
from psycopg2 import ProgrammingError

from airflow.providers.postgres.hooks.postgres import PostgresHook
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCS_BUCKET = "postgres_to_gcs_example"
CREATE_QUERY = """
CREATE TABLE public.test_table
(
    id integer,
    params json
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.test_table
    OWNER to postgres;
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
DELETE_QUERY = "DROP TABLE public.test_table;"


@pytest.mark.backend("postgres")
@pytest.mark.credential_file(GCP_GCS_KEY)
class PostgresToGCSSystemTest(GoogleSystemTest):
    @staticmethod
    def init_db():
        try:
            hook = PostgresHook()
            hook.run(CREATE_QUERY)
            hook.run(LOAD_QUERY)
        except ProgrammingError:
            pass

    @staticmethod
    def drop_db():
        hook = PostgresHook()
        hook.run(DELETE_QUERY)

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(GCS_BUCKET)
        self.init_db()

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_postgres_to_gcs', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(GCS_BUCKET)
        self.drop_db()
        super().tearDown()
