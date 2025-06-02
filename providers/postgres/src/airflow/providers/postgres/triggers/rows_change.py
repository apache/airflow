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
from collections.abc import AsyncIterator
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.triggers.base import BaseEventTrigger, TriggerEvent

# Version guard for Event-driven triggers and Variable (requires Airflow >= 3.0)
try:
    from airflow.sdk import Variable
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "PostgresRowChangeEventTrigger requires Airflow >= 3.0 due to Event-driven scheduling support."
    )


STATE_VARIABLE_PREFIX = "_airflow__postgres_trigger"


class PostgresRowsChangeEventTrigger(BaseEventTrigger):
    """
    PostgresRowsChangeEventTrigger is fired as deferred class with params to run the task in trigger worker.

    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        table_name: str,
        schema_name: str | None = "",
        postgres_conn_id: str | None = "postgres_default",
        poke_interval: float = 120.0,
        **hook_params: Any,
    ):
        super().__init__()
        self.table_name = table_name
        self.schema_name = schema_name
        self.postgres_conn_id = postgres_conn_id
        self.poke_interval = poke_interval
        self.hook_params = hook_params
        self._state_variable = f"{STATE_VARIABLE_PREFIX}-{schema_name}-{table_name}"
        self.__init_state()

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize PostgresRowsChangeEventTrigger arguments and classpath."""
        return (
            "airflow.providers.postgres.triggers.PostgresRowsChangeEventTrigger",
            {
                "table_name": self.table_name,
                "schema_name": self.schema_name,
                "postgres_conn_id": self.postgres_conn_id,
                "poke_interval": self.poke_interval,
                "hook_params": self.hook_params,
                "row_count": self.row_count,
            },
        )

    @property
    def row_count(self):
        state = self.get_state()
        return state.get("row_count")

    # Following methods require further discussion on AIP30 standards/workarounds
    def __init_state(self):
        try:
            self.get_state()
        except AirflowRuntimeError as e:
            if "VARIABLE_NOT_FOUND" in str(e):
                self.update_state({"row_count": 0})
            else:
                raise e

    def update_state(self, state:dict) -> None:
        """Calls external storage to update the latest value (Metadata Database? External Filestore?)"""
        return Variable.set(self._state_variable, state, serialize_json=True)

    def get_state(self) -> dict:
        """Calls external storage to retrieve the latest value (Metadata Database? External Filestore?)"""
        return Variable.get(self._state_variable, deserialize_json=True)

    @cached_property
    def hook(self) -> PostgresHook:
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)

    def _retrieve_row_count(self):
        with self.hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT count(*) FROM {self.schema_name}.{self.table_name}")
                results = cursor.fetchone()
                n_rows = results[0]
                if isinstance(n_rows, int) and n_rows >= 0:
                    return n_rows
        return -1

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async loop, synchronous connection with PostgresHook."""
        try:
            while True:
                n_rows = self._retrieve_row_count()
                if n_rows != -1:
                    self.log.info(f"[{self.table_name}] Rows: {n_rows}")
                    if n_rows > self.row_count:
                        yield TriggerEvent({"status": "running", "operation": "APPEND", "row_count": n_rows})
                    elif n_rows < self.row_count:
                        yield TriggerEvent({"status": "running", "operation": "REMOVE", "row_count": n_rows})

                self.log.info("Sleeping for %s seconds", self.poke_interval)
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
