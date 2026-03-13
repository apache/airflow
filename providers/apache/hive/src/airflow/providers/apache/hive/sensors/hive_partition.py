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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
from airflow.providers.common.compat.sdk import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class HivePartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in Hive.

    Note: Because ``partition`` supports general logical operators, it
    can be inefficient. Consider using NamedHivePartitionSensor instead if
    you don't need the full flexibility of HivePartitionSensor.

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param partition: The partition clause to wait for. This is passed as
        is to the metastore Thrift client ``get_partitions_by_filter`` method,
        and apparently supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``
    :param metastore_conn_id: reference to the
        :ref: `metastore thrift service connection id <howto/connection:hive_metastore>`
    """

    template_fields: Sequence[str] = (
        "schema",
        "table",
        "partition",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        *,
        table: str,
        partition: str | None = "ds='{{ ds }}'",
        metastore_conn_id: str = "metastore_default",
        schema: str = "default",
        poke_interval: int = 60 * 3,
        **kwargs: Any,
    ):
        super().__init__(poke_interval=poke_interval, **kwargs)
        if not partition:
            partition = "ds='{{ ds }}'"
        self.metastore_conn_id = metastore_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self, context: Context) -> bool:
        if "." in self.table:
            self.schema, self.table = self.table.split(".")
        self.log.info("Poking for table %s.%s, partition %s", self.schema, self.table, self.partition)
        if not hasattr(self, "hook"):
            hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        return hook.check_for_partition(self.schema, self.table, self.partition)
