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

from typing import Sequence

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context


class GenericTransfer(BaseOperator):
    """
    Moves data from a connection to another, assuming that they both
    provide the required methods in their respective hooks. The source hook
    needs to expose a `get_records` method, and the destination a
    `insert_rows` method.

    This is meant to be used on small-ish datasets that fit in memory.

    :param sql: SQL query to execute against the source database. (templated)
    :param destination_table: target table. (templated)
    :param source_conn_id: source connection
    :param destination_conn_id: destination connection
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data. (templated)
    :param insert_args: extra params for `insert_rows` method.
    """

    template_fields: Sequence[str] = ("sql", "destination_table", "preoperator")
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
        destination_conn_id: str,
        preoperator: str | list[str] | None = None,
        insert_args: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.preoperator = preoperator
        self.insert_args = insert_args or {}

    def execute(self, context: Context):
        source_hook = BaseHook.get_hook(self.source_conn_id)
        destination_hook = BaseHook.get_hook(self.destination_conn_id)

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
