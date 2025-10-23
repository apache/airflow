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

from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.common.compat.sdk import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CassandraRecordSensor(BaseSensorOperator):
    """
    Checks for the existence of a record in a Cassandra cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CassandraRecordSensor`

    For example, if you want to wait for a record that has values 'v1' and 'v2' for each
    primary keys 'p1' and 'p2' to be populated in keyspace 'k' and table 't',
    instantiate it as follows:

    >>> cassandra_sensor = CassandraRecordSensor(
    ...     table="k.t",
    ...     keys={"p1": "v1", "p2": "v2"},
    ...     cassandra_conn_id="cassandra_default",
    ...     task_id="cassandra_sensor",
    ... )

    :param table: Target Cassandra table.
        Use dot notation to target a specific keyspace.
    :param keys: The keys and their values to be monitored
    :param cassandra_conn_id: The connection ID to use
        when connecting to Cassandra cluster
    """

    template_fields: Sequence[str] = ("table", "keys")

    def __init__(
        self,
        *,
        keys: dict[str, str],
        table: str,
        cassandra_conn_id: str = CassandraHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.cassandra_conn_id = cassandra_conn_id
        self.table = table
        self.keys = keys

    def poke(self, context: Context) -> bool:
        self.log.info("Sensor check existence of record: %s", self.keys)
        hook = CassandraHook(self.cassandra_conn_id)
        return hook.record_exists(self.table, self.keys)
