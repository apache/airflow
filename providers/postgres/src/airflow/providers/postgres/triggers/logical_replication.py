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
        "PostgresReplicationEventTrigger requires Airflow >= 3.0 due to Event-driven scheduling support."
    )


STATE_VARIABLE_PREFIX = "_airflow__postgres_trigger"


class PostgresReplicationEventTrigger(BaseEventTrigger):
    def __init__(
        self,
        replication_slot: str,
        publication: str,
        schema: str | None = None,
        tables: list | None = [],
        postgres_conn_id: str | None = "postgres_default",
        poke_interval: float = 120.0,
        **hook_params: Any,
    ):
        super().__init__()
        self.replication_slot = replication_slot
        self.publication = publication
        self.schema = schema
        self.tables = tables
        self.postgres_conn_id = postgres_conn_id
        self.poke_interval = poke_interval
        self.hook_params = hook_params
        self._state_variable = f"{STATE_VARIABLE_PREFIX}-{replication_slot}-{publication}"
        self._state = {"log_pointer": 0, "log_lsn": 0}
        self.__init_state()

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize PostgresReplicationTrigger arguments and classpath."""
        return (
            "airflow.providers.postgres.triggers.PostgresReplicationTrigger",
            {
                "replication_slot": self.replication_slot,
                "publication": self.publication,
                "schema": self.schema,
                "tables": self.tables,
                "postgres_conn_id": self.postgres_conn_id,
                "poke_interval": self.poke_interval,
                "hook_params": self.hook_params,
                "state": self._state,
            },
        )

    @cached_property
    def hook(self) -> PostgresHook:
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)

    # Following methods require further discussion on AIP30 standards/workarounds
    def __init_state(self):
        try:
            self.__get_state()
        except AirflowRuntimeError as e:
            if "VARIABLE_NOT_FOUND" in str(e):
                self.__update_state(self._state)
            else:
                raise e

    def __update_state(self, state: dict) -> None:
        """Calls external storage to update the latest value (Metadata Database? External Filestore?)"""
        return Variable.set(self._state_variable, state, serialize_json=True)

    def __get_state(self) -> dict:
        """Calls external storage to retrieve the latest value (Metadata Database? External Filestore?)"""
        return Variable.get(self._state_variable, deserialize_json=True)

    def _retrieve_changes(self):
        _query = f"""
            SELECT * FROM pg_logical_slot_peek_binary_changes('{self.slot_name}', null, null, 'publication_names', '{self.publication_name}')
        """
        with self.hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(_query)
                results = cursor.fetchall()
                if isinstance(results, tuple):
                    self._state = self.__get_state()
                    log_pointer = self._state["log_pointer"]
                    return self._process_changes(results[log_pointer:])
            return []

    def _parse_change(self, change: tuple[str], log_index: int) -> dict:
        # WIP
        return {
            "xid": "",
            "operation": "",
            "schema": "",
            "table": "",
            "lsn": 0,
            "metadata": {},
            "log_index": log_index,
        }

    def _verify_filters(self, change: dict) -> dict:
        if self.schema and change["schema"] != self.schema:
            return
        if self.tables and change["table"] not in self.tables:
            return
        if self._state["log_lsn"] > change["lsn"]:
            return
        return True

    def _process_changes(self, changes: list[str] = []):
        log_pointer = self._state["log_pointer"]
        for change_index in range(len(changes)):
            log_index = log_pointer + change_index
            change = self._parse_change(changes[change_index], log_index)
            if self._verify_filters(change):
                yield change
        return []

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an asynchronous connection using PostgresHook."""
        try:
            while True:
                changes = self._retrieve_changes()

                if len(changes) > 0:
                    start, end = changes[0], changes[-1]
                    self._state["log_pointer"] = end["log_index"]
                    self._state["log_lsn"] = end["lsn"]
                    self.__update_state()

                    yield TriggerEvent(
                        {
                            "status": "running",
                            "start_lsn": start["lsn"],
                            "end_lsn": end["lsn"],
                            "metadata": changes,
                        }
                    )

                self.log.info("Sleeping for %s seconds", self.poke_interval)
                await asyncio.sleep(self.poke_interval)

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
