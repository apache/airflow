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

from pinotdb import connect

from airflow.hooks.dbapi_hook import DbApiHook


class PinotDbApiHook(DbApiHook):
    """
    Connect to pinot db(https://github.com/linkedin/pinot) to issue pql
    """
    conn_name_attr = 'pinot_broker_conn_id'
    default_conn_name = 'pinot_broker_default'
    supports_autocommit = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_conn(self):
        """
        Establish a connection to pinot broker through pinot dbqpi.
        """
        conn = self.get_connection(self.pinot_broker_conn_id)
        pinot_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get('endpoint', '/pql'),
            scheme=conn.extra_dejson.get('schema', 'http')
        )
        self.log.info('Get the connection to pinot '
                      'broker on {host}'.format(host=conn.host))
        return pinot_broker_conn

    def get_uri(self):
        """
        Get the connection uri for pinot broker.

        e.g: http://localhost:9000/pql
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        conn_type = 'http' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', 'pql')
        return '{conn_type}://{host}/{endpoint}'.format(
            conn_type=conn_type, host=host, endpoint=endpoint)

    def get_records(self, sql):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_first(self, sql):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def set_autocommit(self, conn, autocommit):
        raise NotImplementedError()

    def get_pandas_df(self, sql, parameters=None):
        raise NotImplementedError()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        raise NotImplementedError()
