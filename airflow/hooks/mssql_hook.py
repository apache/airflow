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

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        conn = self.get_conn()
        if isinstance(sql, basestring):
            sql = [sql]

        if self.supports_autocommit:
            self.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in sql:
            if sys.version_info[0] < 3:
                s = s.encode('utf-8')
            _log.info(s)
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                cur.execute(s)
        # At this point we have received a resultset from SQL into the cursor. There could be more to come
        # as the connection is still open and the sql could well still be running. Since we don't have a
        # use for the resultsets in Airflow right now, loop through resultsets until there are no more.
        while cur.nextset():
            pass
        cur.close()
        conn.commit()
        conn.close()
