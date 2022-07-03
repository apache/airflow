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

from airflow.models import BaseOperator
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class InfluxDBOperator(BaseOperator):
    """
    Executes sql code in a specific InfluxDB database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:InfluxDBOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement
    :param influxdb_conn_id: Reference to :ref:`Influxdb connection id <howto/connection:influxdb>`.
    """

    template_fields: Sequence[str] = ('sql',)

    def __init__(
        self,
        *,
        sql: str,
        influxdb_conn_id: str = 'influxdb_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.influxdb_conn_id = influxdb_conn_id
        self.sql = sql

    def execute(self, context: Context) -> None:
        self.log.info('Executing: %s', self.sql)
        self.hook = InfluxDBHook(conn_id=self.influxdb_conn_id)
        self.hook.query(self.sql)
