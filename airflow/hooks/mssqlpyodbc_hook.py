# -*- coding: utf-8 -*-
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

import pyodbc

from airflow.hooks.dbapi_hook import DbApiHook


class MsSqlPyODBCHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MsSqlPyODBCHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        conn = self.get_connection(self.mssql_conn_id)
        conn = pyodbc.connect(
            "DRIVER={0};SERVER={1};PORT={2};DATABASE={3};UID={4};PWD={5}"
            .format(
                '{ODBC Driver 17 for SQL Server}'
                , conn.host
                , conn.port
                , self.schema or conn.schema
                , conn.login
                , conn.password
            )
        )

        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)