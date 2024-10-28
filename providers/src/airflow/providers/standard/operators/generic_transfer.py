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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GenericTransfer(BaseOperator):
    """
    Moves data from a connection to another.

    Assuming that they both provide the required methods in their respective hooks.
    The source hook needs to expose a `get_records` method, and the destination a
    `insert_rows` method.

    This is meant to be used on small-ish datasets that fit in memory.

    :param sql: SQL query to execute against the source database. (templated)
    :param destination_table: target table. (templated)
    :param source_conn_id: source connection. (templated)
    :param destination_conn_id: destination connection. (templated)
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data. (templated)
    :param insert_args: extra params for `insert_rows` method.
    """

    template_fields: Sequence[str] = (
        "source_conn_id",
        "destination_conn_id",
        "sql",
        "destination_table",
        "preoperator",
        "insert_args",
    )
    template_ext: Sequence[str] = (
        ".sql",
        ".hql",
    )
    template_fields_renderers = {"preoperator": "sql"}
    ui_color = "#b0f07c"

    def __init__(
        self,
        *,
        sql: str,
        destination_table: str,
        source_conn_id: str,
        source_hook_params: dict | None = None,
        destination_conn_id: str,
        destination_hook_params: dict | None = None,
        preoperator: str | list[str] | None = None,
        insert_args: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.source_hook_params = source_hook_params
        self.destination_conn_id = destination_conn_id
        self.destination_hook_params = destination_hook_params
        self.preoperator = preoperator
        self.insert_args = insert_args or {}

    @classmethod
    def get_hook(cls, conn_id: str, hook_params: dict | None = None) -> BaseHook:
        """
        Return default hook for this connection id.

        :param conn_id: connection id
        :param hook_params: hook parameters
        :return: default hook for this connection
        """
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook(hook_params=hook_params)

    def execute(self, context: Context):
        source_hook = self.get_hook(
            conn_id=self.source_conn_id, hook_params=self.source_hook_params
        )
        destination_hook = self.get_hook(
            conn_id=self.destination_conn_id, hook_params=self.destination_hook_params
        )

        self.log.info("Extracting data from %s", self.source_conn_id)
        self.log.info("Executing: \n %s", self.sql)
        get_records = getattr(source_hook, "get_records", None)
        if not callable(get_records):
            raise RuntimeError(
                f"Hook for connection {self.source_conn_id!r} "
                f"({type(source_hook).__name__}) has no `get_records` method"
            )
        else:
            results = get_records(self.sql)

        if self.preoperator:
            run = getattr(destination_hook, "run", None)
            if not callable(run):
                raise RuntimeError(
                    f"Hook for connection {self.destination_conn_id!r} "
                    f"({type(destination_hook).__name__}) has no `run` method"
                )
            self.log.info("Running preoperator")
            self.log.info(self.preoperator)
            run(self.preoperator)

        insert_rows = getattr(destination_hook, "insert_rows", None)
        if not callable(insert_rows):
            raise RuntimeError(
                f"Hook for connection {self.destination_conn_id!r} "
                f"({type(destination_hook).__name__}) has no `insert_rows` method"
            )
        self.log.info("Inserting rows into %s", self.destination_conn_id)
        insert_rows(table=self.destination_table, rows=results, **self.insert_args)
