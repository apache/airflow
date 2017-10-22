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

from builtins import str

import jaydebeapi

from airflow.hooks.dbapi_hook import DbApiHook


class JdbcHook(DbApiHook):
    """
    General hook for jdbc db access.

    If a connection id is specified, host, port, schema, username and password will be taken from the predefined connection.
    Raises an airflow error if the given connection id doesn't exist.
    Otherwise host, port, schema, username and password can be specified on the fly.

    :param jdbc_url: jdbc connection url
    :type jdbc_url: string
    :param jdbc_driver_name: jdbc driver name
    :type jdbc_driver_name: string
    :param jdbc_driver_loc: path to jdbc driver
    :type jdbc_driver_loc: string
    :param conn_id: reference to a predefined database
    :type conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    conn_name_attr = 'jdbc_conn_id'
    default_conn_name = 'jdbc_default'
    supports_autocommit = True

    def get_conn(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        login = conn.login
        psw = conn.password
        jdbc_driver_loc = conn.extra_dejson.get('extra__jdbc__drv_path')
        jdbc_driver_name = conn.extra_dejson.get('extra__jdbc__drv_clsname')

        conn = jaydebeapi.connect(jclassname=jdbc_driver_name,
                                  url=str(host),
                                  driver_args=[str(login), str(psw)],
                                  jars=jdbc_driver_loc.split(","))
        return conn

    def set_autocommit(self, conn, autocommit):
        """
        Enable or disable autocommit for the given connection.

        :param conn: The connection
        :return:
        """
        conn.jconn.autocommit = autocommit
