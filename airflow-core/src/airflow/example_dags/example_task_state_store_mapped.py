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
"""Example DAG with mapped tasks to demonstrate task state store isolation per map_index."""

from __future__ import annotations

import random
from datetime import datetime, timezone

from airflow.sdk import DAG, task

TABLES = ["orders", "customers", "products"]

with DAG(
    dag_id="example_task_state_store_mapped",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "task-state-store"],
    doc_md=__doc__,
) as dag:

    @task
    def get_tables() -> list[str]:
        """Return the list of tables to process."""
        return TABLES

    @task
    def process_table(table: str, task_state_store=None, ti=None) -> dict:
        """Process one table — each mapped instance gets its own task state."""
        row_count = random.randint(100, 10000)
        result = {
            "table": table,
            "map_index": ti.map_index,
            "row_count": row_count,
            "processed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        }
        task_state_store.set("status", "complete")
        task_state_store.set("result", result)

        print(f"[map_index={ti.map_index}] Processed {table}: {row_count} rows")
        return result

    tables = get_tables()
    process_table.expand(table=tables)
