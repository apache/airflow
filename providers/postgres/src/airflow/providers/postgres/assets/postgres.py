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

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from urllib.parse import SplitResult


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.netloc:
        raise ValueError("URI format postgres:// must contain a host")
    if uri.port is None:
        host = uri.netloc.rstrip(":")
        uri = uri._replace(netloc=f"{host}:5432")
    path_parts = uri.path.split("/")
    if len(path_parts) != 4:  # Leading slash, database, schema, and table names.
        raise ValueError("URI format postgres:// must contain database, schema, and table names")
    if not path_parts[2]:
        path_parts[2] = "default"
    return uri._replace(scheme="postgres", path="/".join(path_parts))


class PostgresReplicationTrigger(BaseTrigger):
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
        self._state = {"log_pointer": 0, "log_lsn": 0}

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
                "latest_count": self._latest_count
            },
        )

    @cached_property
    def hook(self) -> PostgresHook:
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id
        )


    # Following methods require further discussion on AIP30 standards/workarounds
    def __update_state(self) -> None:
        """Calls external storage to update the latest value (Metadata Database? External Filestore?)"""
        # Using the self._state attribute
        raise NotImplementedError

    def __get_state(self) -> dict:
        """Calls external storage to retrieve the latest value (Metadata Database? External Filestore?)"""
        raise NotImplementedError


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
            "xid": str(),
            "operation": str(),
            "schema": str(),
            "table": str(),
            "lsn": int(),
            "metadata": dict(),
            "log_index": log_index
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
                
                lsn, index = 0, 0
                for change in changes:
                    _, operation, _, table, lsn, metadata, index = change.values()
                    yield TriggerEvent({"status": "running", "operation": operation, "table": table, "lsn": lsn, "metadata": metadata})
                
                if len(changes) > 0:
                    self._state["log_pointer"] = index
                    self._state["log_lsn"] = lsn
                    self.__update_state()

                self.log.info("Sleeping for %s seconds", self.poke_interval)
                await asyncio.sleep(self.poke_interval)

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})



class PostgresRowsChangeTrigger(BaseTrigger):
    """
    PostgresRowsChangeTrigger is fired as deferred class with params to run the task in trigger worker.

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

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize PostgresRowsChangeTrigger arguments and classpath."""
        return (
            "airflow.providers.postgres.triggers.PostgresRowsChangeTrigger",
            {
                "table_name": self.table_name,
                "schema_name": self.schema_name,
                "postgres_conn_id": self.postgres_conn_id,
                "poke_interval": self.poke_interval,
                "hook_params": self.hook_params,
                "row_count": self.row_count
            }
        )

    @property
    def row_count(self):
        state = self.__get_state()
        return state.get("row_count")

    # Following methods require further discussion on AIP30 standards/workarounds
    def __update_state(self, state: dict) -> None:
        """Calls external storage to update the latest value (Metadata Database? External Filestore?)"""
        raise NotImplementedError

    def __get_state(self) -> dict:
        """Calls external storage to retrieve the latest value (Metadata Database? External Filestore?)"""
        raise NotImplementedError


    @cached_property
    def hook(self) -> PostgresHook:
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id
        )

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
                    if n_rows > self._latest_count:
                        yield TriggerEvent({"status": "running", "operation": "APPEND", "row_count": n_rows})
                    elif n_rows < self._latest_count:
                        yield TriggerEvent({"status": "running", "operation": "REMOVE", "row_count": n_rows})
                    self.__update_state({"row_count": n_rows})

                self.log.info("Sleeping for %s seconds", self.poke_interval)
                await asyncio.sleep(self.poke_interval)

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
