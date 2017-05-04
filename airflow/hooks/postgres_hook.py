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

import psycopg2
import psycopg2.extensions

from airflow.hooks.dbapi_hook import DbApiHook


class PostgresHook(DbApiHook):
    """
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    """
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'sslcrl', 'application_name']:
                conn_args[arg_name] = arg_val
        psycopg2_conn = psycopg2.connect(**conn_args)
        return psycopg2_conn

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Returns the Postgres literal of the cell as a string.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The serialized cell
        :rtype: str
        """

        return psycopg2.extensions.adapt(cell).getquoted().decode('utf-8')
