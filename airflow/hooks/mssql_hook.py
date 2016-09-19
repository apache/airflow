# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pymssql

from airflow.hooks.dbapi_hook import DbApiHook


class MsSqlHook(DbApiHook):
    '''
    Interact with Microsoft SQL Server.
    '''

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        conn = self.get_connection(self.mssql_conn_id)
        conn = pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema,
            port=conn.port)
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)
