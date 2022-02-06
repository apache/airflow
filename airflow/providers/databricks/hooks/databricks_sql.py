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

import re
from contextlib import closing
from copy import copy
from typing import Any, Dict, List, Optional, Union

from databricks import sql
from databricks.sql.client import Connection

from airflow.exceptions import AirflowException
from airflow.hooks.dbapi import DbApiHook
from airflow.providers.databricks.hooks.databricks_base import DatabricksBaseHook


class DatabricksSqlHook(DatabricksBaseHook, DbApiHook):
    """
    Interact with Databricks SQL.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be specified in the Databricks connection's extra parameters.
    :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
        If not specified, it could be specified in the Databricks connection's extra parameters.
    """

    conn_name_attr = 'databricks_conn_id'
    default_conn_name = 'databricks_default'
    conn_type = 'databricks'
    hook_name = 'Databricks SQL'

    def __init__(
        self,
        databricks_conn_id: str = default_conn_name,
        http_path: Optional[str] = None,
        session_configuration: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(databricks_conn_id)
        self._sql_conn = None
        self._token = None
        self._http_path = http_path
        self.supports_autocommit = True
        self.session_config = session_configuration

    def _get_extra_config(self) -> Dict[str, Optional[Any]]:
        extra_params = copy(self.databricks_conn.extra_dejson)
        for arg in ['http_path', 'session_configuration'] + self.extra_parameters:
            if arg in extra_params:
                del extra_params[arg]

        return extra_params

    def get_conn(self) -> Connection:
        """Returns a Databricks SQL connection object"""
        if not self._http_path:
            if 'http_path' not in self.databricks_conn.extra_dejson:
                raise AirflowException(
                    "http_path should be provided either explicitly or in extra parameter of Databricks connection"
                )
            self._http_path = self.databricks_conn.extra_dejson['http_path']
        requires_init = True
        if not self._token:
            self._token = self._get_token(raise_error=True)
        else:
            new_token = self._get_token(raise_error=True)
            if new_token != self._token:
                self._token = new_token
            else:
                requires_init = False

        if not self.session_config:
            self.session_config = self.databricks_conn.extra_dejson.get('session_configuration')

        if not self._sql_conn or requires_init:
            self._sql_conn = sql.connect(
                self.host,
                self._http_path,
                self._token,
                session_configuration=self.session_config,
                **self._get_extra_config(),
            )
        return self._sql_conn

    @staticmethod
    def maybe_split_sql_string(sql: str) -> List[str]:
        """
        Splits strings consisting of multiple SQL expressions into an
        TODO: do we need something more sophisticated?

        :param sql: SQL string potentially consisting of multiple expressions
        :return: list of individual expressions
        """
        splits = [s.strip() for s in re.split(";\\s*\r?\n", sql) if s.strip() != ""]
        return splits

    def run(self, sql: Union[str, List[str]], autocommit=True, parameters=None, handler=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :return: query results.
        """
        if isinstance(sql, str):
            sql = self.maybe_split_sql_string(sql)
        self.log.debug("Executing %d statements", len(sql))

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                for sql_statement in sql:
                    self.log.info("Executing statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute(sql_statement, parameters)
                    else:
                        cur.execute(sql_statement)

                    results = []
                    if handler is not None:
                        cur = handler(cur)
                    for row in cur:
                        self.log.debug("Statement results: %s", row)
                        results.append(row)

                    self.log.info("Rows affected: %s", cur.rowcount)

        # Return only result of the last SQL expression
        return results

    def test_connection(self):
        """Test the Databricks SQL connection by running a simple query."""
        try:
            self.run(sql="select 42")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully checked"

    def bulk_dump(self, table, tmp_file):
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        raise NotImplementedError()
