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
from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.pgvector.hooks.pgvector import PgVectorHook
from airflow.providers.pgvector.operators.pgvector import PgVectorIngestOperator

TABLE_NAME = "my_table"
POSTGRES_CONN_ID = "postgres_default"


@dag(
    "example_pgvector_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "pgvector"],
)
def example_pgvector_dag():
    """Example pgvector DAG demonstrating usage of the PgVectorIngestOperator."""

    @task()
    def create_postgres_objects():
        """
        Example task to create PostgreSQL objects including table and installing the vector extension using
        the PgVectorHook.
        """
        pg_hook = PgVectorHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Create a table
        columns = [
            "id SERIAL PRIMARY KEY",
            "name VARCHAR(255)",
            "value INTEGER",
            "vector_column vector(3)",
        ]
        pg_hook.create_table(TABLE_NAME, columns)

        # Create vector extension
        extension_name = "vector"
        pg_hook.create_extension(extension_name)

    # [START howto_operator_pgvector_ingest]
    pgvector_ingest = PgVectorIngestOperator(
        task_id="pgvector_ingest",
        conn_id=POSTGRES_CONN_ID,
        sql=f"INSERT INTO {TABLE_NAME} (name, value, vector_column) "
        f"VALUES ('John Doe', 123, '[1.0, 2.0, 3.0]')",
    )
    # [END howto_operator_pgvector_ingest]

    @task()
    def cleanup_postgres_objects():
        """
        Cleanup Postgres objects created in the earlier task.
        """
        pg_hook = PgVectorHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.truncate_table(TABLE_NAME)
        pg_hook.drop_table(TABLE_NAME)

    create_postgres_objects() >> pgvector_ingest >> cleanup_postgres_objects()


example_pgvector_dag()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
