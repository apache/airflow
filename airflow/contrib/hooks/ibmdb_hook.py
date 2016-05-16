# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.hooks.dbapi_hook import DbApiHook

import ibm_db
import ibm_db_dbi


class IbmDbHook(DbApiHook):
    """Interact with IBM's DB2 family of databases."""

    conn_name_attr = 'ibmdb_conn_id'
    default_conn_name = 'ibmdb_default'
    supports_autocommit = True

    _CONN_STR_TEMPLATE = ('DATABASE={schema};'
                          'HOSTNAME={hostname};'
                          'PORT={port};'
                          'UID={username};'
                          'PWD={password};')

    def get_conn(self):
        """Returns an IBM DB2 connection object."""
        conn = self.get_connection(self.ibmdb_conn_id)

        conn_info = {
            'username': conn.login,
            'password': conn.password,
            'schema': conn.schema,
            'hostname': conn.host or 'localhost',
            'port': conn.port or '50000'
        }

        ibm_db_conn = ibm_db.connect(
            self._CONN_STR_TEMPLATE.format(**conn_info),
            '',
            ''
        )

        return ibm_db_dbi.Connection(ibm_db_conn)
