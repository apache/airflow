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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

from datetime import timedelta

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.triggers.hive_partition import HivePartitionTrigger


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


class HivePartitionAsyncSensor(HivePartitionSensor):
    """
    Waits for a given partition to show up in Hive table asynchronously.

    .. note::
       HivePartitionSensorAsync uses impyla library instead of PyHive.
       The sync version of this sensor uses `PyHive <https://github.com/dropbox/PyHive>`.

       Since we use `impyla <https://github.com/cloudera/impyla>`_ library,
       please set the connection to use the port ``10000`` instead of ``9083``.
       For ``auth_mechanism='GSSAPI'`` the ticket renewal happens through command
       ``airflow kerberos`` in
       `worker/trigger <https://airflow.apache.org/docs/apache-airflow/stable/security/kerberos.html>`_.

       You may also need to allow traffic from Airflow worker/Triggerer to the Hive instance,
       depending on where they are running. For example, you might consider adding an entry in the
       ``etc/hosts`` file present in the Airflow worker/Triggerer, which maps the EMR Master node
       Public IP Address to its Private DNS Name to allow the network traffic.

       The library version of hive and hadoop in ``Dockerfile`` should match the remote
       cluster where they are running.

    :param table: the table where the partition is present.
    :param partition: The partition clause to wait for. This is passed as
        notation as in "ds='2015-01-01'"
    :param schema: database which needs to be connected in hive. By default, it is 'default'
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_interval: The interval in seconds to wait between checks for partition.
    """

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=HivePartitionTrigger(
                table=self.table,
                schema=self.schema,
                partition=self.partition,
                polling_interval=self.poke_interval,
                metastore_conn_id=self.metastore_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str] | None = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                self.log.info(
                    "Success criteria met. Found partition %s in table: %s", self.partition, self.table
                )
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
