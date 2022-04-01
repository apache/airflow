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

"""This module allows connecting to a ClickHouse."""
from typing import Any, Dict, Optional

from clickhouse_driver import Client as ClickHouseClient

from airflow import AirflowException
from airflow.hooks.base import BaseHook


class ClickHouseHook(BaseHook):
    """
    Interact with ClickHouse.

    Performs a connection to ClickHouse and retrieves client.

    :param clickhouse_conn_id: Reference to :ref:`ClickHouse connection id <howto/connection:clickhouse>`.
    :param database:Optional[str], database for the hook, if not provided schema from Connection will be
    used`.
    """

    conn_name_attr = 'clickhouse_conn_id'
    default_conn_name = 'clickhouse_default'
    conn_type = 'clickhouse'
    hook_name = 'ClickHouse'

    def __init__(self, clickhouse_conn_id: str = default_conn_name, database: Optional[str] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database

        self.client: Optional[ClickHouseClient] = None
        self.get_conn()

    def get_conn(self) -> ClickHouseClient:
        """Function that initiates a new ClickHouse connection"""
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.clickhouse_conn_id)
        connection_kwargs = conn.extra_dejson.copy()

        if conn.port:
            connection_kwargs.update(port=int(conn.port))
        if conn.login:
            connection_kwargs.update(user=conn.login)
        if conn.password:
            connection_kwargs.update(password=conn.password)

        #  if custom database is provided use it or use from schema
        if self.database:
            connection_kwargs.update(database=self.database)
        elif conn.schema:
            connection_kwargs.update(database=conn.schema)

        self.client = ClickHouseClient(conn.host or 'localhost', **connection_kwargs)
        return self.client

    def query(self, query, **kwargs) -> Any:
        """
        Function to create a clickhouse session
        and execute the AQL query in the session.

        :param query: AQL query
        :return: Result
        """
        try:
            if self.db_conn:
                result = self.db_conn.aql.execute(query, **kwargs)
                return result
            else:
                raise AirflowException(
                    f"Failed to execute AQLQuery, error connecting to database: {self.database}"
                )
        except Exception as error:
            raise AirflowException(f"Failed to execute AQLQuery, error: {str(error)}")

    def create_database(self, name):
        if not self.db_conn.has_database(name):
            self.db_conn.create_database(name)
            return True
        else:
            self.log.info('Database already exists: %s', name)
            return False

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        import json
        return {
            "relabeling": {
                'host': 'ClickHouse Host',
                'port': 'ClickHouse Port',
                'schema': 'ClickHouse Database',
                'login': 'ClickHouse Username',
                'password': 'ClickHouse Password',
                'extra': 'extra configs (https://clickhouse-driver.readthedocs.io/en/latest/api.html'
                         '#connection) '
            },
            "placeholders": {
                'host': 'http://127.0.0.1',
                'port': '8123',
                'schema': 'default',
                'login': 'root',
                'password': 'password',
                'extra': json.dumps(
                    {
                        "key for config": "value for config",
                    },
                    indent=1,
                ),
            },
        }
