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
from typing import Optional, Dict, Any, Callable

from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.sensors.sql import SqlSensor


class ClickHouseSensor(SqlSensor):
    """
    Checks for the existence of a document which
    matches the given query in ClickHouse. Example:

    :param sql: The query to poke, or you can provide .sql file having the query
    :param parameters: The parameters to render the SQL query with (optional).
    :param database: Database to query, if not provided schema from Connection will be used (optional).
    :param success: Success criteria for the sensor is a Callable that takes first_cell
        as the only argument, and returns a boolean (optional).
    :param failure: Failure criteria for the sensor is a Callable that takes first_cell
        as the only argument and return a boolean (optional).
    :param fail_on_empty: Explicitly fail on no rows returned.
    :param clickhouse_conn_id: The :ref:`ClickHouse connection id<howto/connection:clickhouse>` to use
        when connecting to ClickHouse.
    """

    ui_color = '#a3985f'

    def __init__(self,
                 sql: str,
                 parameters: Optional[Dict[str, Any]] = None,
                 database: Optional[str] = None,
                 success: Optional[Callable[[Any], bool]] = None,
                 failure: Optional[Callable[[Any], bool]] = None,
                 fail_on_empty: bool = False,
                 clickhouse_conn_id: str = "clickhouse_default",
                 *args,
                 **kwargs) -> None:
        super().__init__(
            conn_id=clickhouse_conn_id,
            sql=sql,
            parameters=parameters,
            success=success,
            failure=failure,
            fail_on_empty=fail_on_empty,
            *args,
            **kwargs,
        )
        self.database = database

    def _get_hook(self) -> ClickHouseHook:
        return ClickHouseHook(
            clickhouse_conn_id=self.conn_id,
            database=self.database,
        )
