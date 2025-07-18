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

from datetime import datetime, timezone

import psycopg2

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.hooks.base_hook import BaseHook

# Version guard for Airflow 3.0+ due to AssetWatcher and EventTrigger support
try:
    from airflow.decorators import task
    from airflow.models import DAG
    from airflow.providers.postgres.triggers.postgres_cdc import PostgresCDCEventTrigger
    from airflow.sdk import Asset, AssetWatcher, Variable
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This DAG requires Airflow >= 3.0 due to AssetWatcher and Event-driven trigger support."
    )

"""
Example DAG that demonstrates how to use the PostgresCDCEventTrigger.

This DAG sets up an AssetWatcher linked to a PostgreSQL CDC trigger, which monitors changes
in a specific table based on a timestamp column (`cdc_column`). When a change is detected, it executes a task
that processes new rows and updates the Airflow Variable used to track CDC state (`state_key`).

**Important**: The state Variable must be explicitly initialized before using the trigger to prevent
recursive DAG executions. This example shows how to do this properly.

The state variable is configurable via `state_key`.
"""

STATE_KEY = "my_table_cdc_last_value"

# [START howto_operator_postgres_cdc_watcher]
# Note: For high load tables, consider the frequency of CDC events.
# Polling-based CDC may result in many events and load on Airflow.
cdc_trigger = PostgresCDCEventTrigger(
    conn_id="postgres_default",
    table="my_table",
    cdc_column="updated_at",
    polling_interval=20,
    state_key=STATE_KEY,
)

cdc_asset = Asset(
    "postgres_cdc_asset", watchers=[AssetWatcher(name="postgres_cdc_watcher", trigger=cdc_trigger)]
)

with DAG(
    dag_id="example_postgres_cdc_watcher",
    schedule=[cdc_asset],
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def initialize_cdc_state():
        """
        Initialize the CDC state Variable if it doesn't exist.
        This prevents recursive DAG executions by ensuring the Variable is set before the trigger runs.
        """
        try:
            current_value = Variable.get(STATE_KEY, default=None)
            if current_value is None:
                # Initialize with a timestamp before any expected data
                initial_timestamp = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                Variable.set(STATE_KEY, initial_timestamp.isoformat())
                print(f"Initialized {STATE_KEY} with: {initial_timestamp.isoformat()}")
            else:
                print(f"CDC state Variable {STATE_KEY} already exists with value: {current_value}")
        except Exception as e:
            print(f"Error initializing CDC state: {e}")
            raise

    @task
    def extract_and_update_last_value():
        """
        Extracts new rows from the monitored table that have `updated_at` greater than the
        last recorded value in Airflow Variable, then updates the Variable with the new maximum.
        """
        conn = BaseHook.get_connection("postgres_default")

        with psycopg2.connect(
            host=conn.host, port=conn.port, dbname=conn.schema, user=conn.login, password=conn.password
        ) as pg_conn:
            with pg_conn.cursor() as cursor:
                last_value = Variable.get(STATE_KEY, default=None)
                if last_value:
                    last_dt = datetime.fromisoformat(last_value)
                else:
                    # This should not happen if initialize_cdc_state ran properly
                    raise ValueError(
                        f"CDC state Variable {STATE_KEY} not found. Please run initialize_cdc_state task first."
                    )

                query = "SELECT updated_at, id, data FROM my_table WHERE updated_at > %s"
                cursor.execute(query, (last_dt,))
                results = cursor.fetchall()

                if results:
                    print(f"Found {len(results)} new updates.")
                    updated_ats = [row[0] for row in results]
                    max_updated_at = max(updated_ats)
                    Variable.set(STATE_KEY, max_updated_at.isoformat())
                    print(f"Updated {STATE_KEY} to: {max_updated_at.isoformat()}")
                    print(f"Rows: {results}")
                else:
                    print("No new updates found.")

    # Initialize state before processing
    initialize_cdc_state() >> extract_and_update_last_value()
# [END howto_operator_postgres_cdc_watcher]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
