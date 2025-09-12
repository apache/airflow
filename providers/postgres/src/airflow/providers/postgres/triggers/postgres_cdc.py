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

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Version guard for Event-driven triggers and Variable (requires Airflow >= 3.0)
try:
    from airflow.sdk import Variable
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "PostgresCDCEventTrigger requires Airflow >= 3.0 due to Event-driven scheduling support."
    )


class PostgresCDCEventTrigger(BaseEventTrigger):
    """
    A trigger that waits for changes in a PostgreSQL table using polling over a timestamp/version column.

    The behavior of this trigger is as follows:

    - Check if the state Variable exists. If not, log a warning and skip event emission to prevent recursive DAG executions.
    - Poll the target table using `SELECT MAX(cdc_column)`.
    - Compare the result with the stored "last read" value (persisted as an Airflow Variable).
    - If a new value is found (greater than last read), emit a TriggerEvent containing metadata.
    - If no new value is found, sleep for `polling_interval` seconds and continue polling.

    **Important**: The state Variable must be created before using this trigger (via UI, CLI, or code).
    Your DAG must also update this Variable after processing changes to prevent reprocessing the same data.
    If the Variable is not found, the trigger will log a warning and skip event emission to avoid recursive DAG executions.

    Example usage:
        .. code-block:: python

            from airflow.sdk import Variable
            from airflow.providers.postgres.triggers.postgres_cdc import PostgresCDCEventTrigger

            # Create the state Variable (do this once, via UI, CLI, or code)
            Variable.set("users_cdc_last_value", "2024-01-01T00:00:00+00:00")

            # Use the trigger
            trigger = PostgresCDCEventTrigger(
                conn_id="my_postgres", table="users", cdc_column="updated_at", polling_interval=30.0
            )


            # In your DAG task that processes the changes:
            @task
            def process_changes():
                # Get the last processed timestamp
                last_value = Variable.get("users_cdc_last_value")

                # Process new data since last_value
                # ... your processing logic here ...

                # IMPORTANT: Update the Variable with the new timestamp
                # to prevent reprocessing the same data
                Variable.set("users_cdc_last_value", "2024-01-15T10:30:00+00:00")

    :param conn_id: Airflow connection ID for PostgreSQL.
    :param table: Name of the table to monitor.
    :param cdc_column: Column representing the change timestamp or version.
    :param polling_interval: Time (seconds) between polling attempts (default: 10.0).
    :param state_key: Key used for persisting CDC state. If None, defaults to `{table}_cdc_last_value`.
    """

    def __init__(
        self,
        conn_id: str,
        table: str,
        cdc_column: str,
        polling_interval: float = 10.0,
        state_key: str | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.table = table
        self.cdc_column = cdc_column
        self.polling_interval = polling_interval
        self._state_key = state_key or f"{self.table}_cdc_last_value"

    @property
    def state_key(self) -> str:
        return self._state_key

    async def get_state(self) -> str | None:
        """Get the last processed state value from Airflow Variables."""
        try:
            if hasattr(Variable, "aget"):
                return await Variable.aget(self.state_key, default=None)
            return Variable.get(self.state_key, default=None)
        except Exception as e:
            self.log.error("Error getting state Variable '%s': %s", self.state_key, e)
            return None

    def serialize(self) -> tuple[str, dict]:
        return (
            "airflow.providers.postgres.triggers.postgres_cdc.PostgresCDCEventTrigger",
            {
                "conn_id": self.conn_id,
                "table": self.table,
                "cdc_column": self.cdc_column,
                "polling_interval": self.polling_interval,
                "state_key": self.state_key,
            },
        )

    async def run(self) -> AsyncGenerator[TriggerEvent, None]:
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        # Check if state Variable exists to prevent recursive DAG executions
        last_value = await self.get_state()
        if last_value is None:
            self.log.warning(
                "State Variable '%s' not found. Please create this Variable (via UI, CLI, or code) "
                "and ensure your DAG updates it after processing changes to prevent reprocessing the same data. "
                "Example: Variable.set('%s', '2024-01-01T00:00:00+00:00'). Skipping event emission.",
                self.state_key,
                self.state_key,
            )
            # Wait for the polling interval and continue polling
            self.log.info("Sleeping for %s seconds", self.polling_interval)
            await asyncio.sleep(self.polling_interval)
            return

        while True:
            try:
                with hook.get_conn() as pg_conn:
                    with pg_conn.cursor() as cursor:
                        cursor.execute(f"SELECT MAX({self.cdc_column}) FROM {self.table}")
                        result = cursor.fetchone()
                        max_value: datetime | None = result[0]

                        if not max_value:
                            self.log.info("No data found in %s for column %s.", self.table, self.cdc_column)
                            return

                        if max_value.tzinfo is None:
                            max_value = max_value.replace(tzinfo=timezone.utc)

                        max_iso = max_value.isoformat()

                        # Re-fetch state in case it was updated during polling
                        last_value = await self.get_state()
                        if last_value:
                            last_dt = datetime.fromisoformat(last_value)
                            if last_dt.tzinfo is None:
                                last_dt = last_dt.replace(tzinfo=timezone.utc)
                        else:
                            # This should not happen since we checked above, but handle gracefully
                            self.log.warning(
                                "State Variable '%s' was removed during polling. Skipping event emission.",
                                self.state_key,
                            )
                            await asyncio.sleep(self.polling_interval)
                            return

                        if max_value > last_dt:
                            self.log.info("New change detected: %s (iso: %s)", max_value, max_iso)
                            self.log.info(
                                "IMPORTANT: After processing changes in your DAG, remember to update "
                                "Variable '%s' with the new timestamp to prevent reprocessing the same data.",
                                self.state_key,
                            )
                            yield TriggerEvent(
                                {"message": f"New change detected at {max_value}", "max_iso": max_iso}
                            )
                            await asyncio.sleep(self.polling_interval)
                            return
                        else:
                            self.log.info("No new change. max: %s, last: %s", max_value, last_dt)

            except Exception as e:
                self.log.error("Error during CDC polling: %s", e)

            self.log.info("Sleeping for %s seconds", self.polling_interval)
            await asyncio.sleep(self.polling_interval)
