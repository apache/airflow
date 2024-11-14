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
"""This module contains an operator to move data from Vertica to Hive."""

from __future__ import annotations

import csv
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class VerticaToHiveOperator(BaseOperator):
    """
    Moves data from Vertica to Hive.

    The operator runs your query against Vertica, stores the file
    locally before loading it into a Hive table. If the ``create``
    or ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata.
    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the table gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param sql: SQL query to execute against the Vertica database. (templated)
    :param hive_table: target Hive table, use dot notation to target a
        specific database. (templated)
    :param create: whether to create the table if it doesn't exist
    :param recreate: whether to drop and recreate the table at every execution
    :param partition: target partition as a dict of partition columns
        and values. (templated)
    :param delimiter: field delimiter in the file
    :param vertica_conn_id: source Vertica connection
    :param hive_cli_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`.
    :param hive_auth: optional authentication option passed for the Hive connection
    """

    template_fields: Sequence[str] = ("sql", "partition", "hive_table")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#b4e0ff"

    def __init__(
        self,
        *,
        sql: str,
        hive_table: str,
        create: bool = True,
        recreate: bool = False,
        partition: dict | None = None,
        delimiter: str = chr(1),
        vertica_conn_id: str = "vertica_default",
        hive_cli_conn_id: str = "hive_cli_default",
        hive_auth: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.hive_table = hive_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = str(delimiter)
        self.vertica_conn_id = vertica_conn_id
        self.hive_cli_conn_id = hive_cli_conn_id
        self.partition = partition or {}
        self.hive_auth = hive_auth

    @classmethod
    def type_map(cls, vertica_type):
        """
        Manually hack Vertica-Python type mapping.

        The stock datatype.py does not provide the full type mapping access.

        Reference:
        https://github.com/uber/vertica-python/blob/master/vertica_python/vertica/column.py
        """
        type_map = {
            5: "BOOLEAN",
            6: "INT",
            7: "FLOAT",
            8: "STRING",
            9: "STRING",
            16: "FLOAT",
        }
        return type_map.get(vertica_type, "STRING")

    def execute(self, context: Context):
        hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id, auth=self.hive_auth)
        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)

        self.log.info("Dumping Vertica query results to local file")
        conn = vertica.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        with NamedTemporaryFile(mode="w", encoding="utf-8") as f:
            csv_writer = csv.writer(f, delimiter=self.delimiter)
            field_dict = {}
            for col_count, field in enumerate(cursor.description, start=1):
                col_position = f"Column{col_count}"
                field_dict[col_position if field[0] == "" else field[0]] = self.type_map(field[1])
            csv_writer.writerows(cursor.iterate())
            f.flush()
            cursor.close()
            conn.close()
            self.log.info("Loading file into Hive")
            hive.load_file(
                f.name,
                self.hive_table,
                field_dict=field_dict,
                create=self.create,
                partition=self.partition,
                delimiter=self.delimiter,
                recreate=self.recreate,
            )
