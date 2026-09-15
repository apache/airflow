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
"""
Example Dag demonstrating :class:`~airflow.providers.apache.arrow.hooks.adbc.AdbcHook`.

Prerequisites
-------------
An Airflow connection of type ``adbc`` must exist with conn_id ``adbc_sqlite_default``:

.. code-block:: json

    {
      "conn_id": "adbc_sqlite_default",
      "conn_type": "adbc",
      "host": "file::memory:?cache=shared",
      "extra": {
        "driver": "adbc_driver_sqlite"
      }
    }
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_adbc"
CONN_ID = "adbc_sqlite_default"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_adbc_hook]
    @dag.task
    def create_table():
        from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

        hook = AdbcHook(adbc_conn_id=CONN_ID)
        hook.run("CREATE TABLE IF NOT EXISTS users (  id   INTEGER PRIMARY KEY,  name TEXT NOT NULL)")

    @dag.task
    def insert_rows():
        from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

        hook = AdbcHook(adbc_conn_id=CONN_ID)
        rows = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
        hook.insert_rows(table="users", rows=rows, target_fields=["id", "name"])

    @dag.task
    def query_rows():
        from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

        hook = AdbcHook(adbc_conn_id=CONN_ID)
        records = hook.get_records("SELECT id, name FROM users ORDER BY id")
        assert len(records) == 3, f"Expected 3 rows, got {len(records)}"

    @dag.task
    def drop_table():
        from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

        hook = AdbcHook(adbc_conn_id=CONN_ID)
        hook.run("DROP TABLE IF EXISTS users")

    create_table() >> insert_rows() >> query_rows() >> drop_table()
    # [END howto_adbc_hook]

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
