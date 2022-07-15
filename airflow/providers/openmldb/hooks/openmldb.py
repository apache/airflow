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

from airflow.providers.http.hooks.http import HttpHook


class OpenMLDBHook(HttpHook):
    """
    Hook for OpenMLDB API Server

    :param openmldb_conn_id: Required. The name of the OpenMLDB connection to get
        connection information for OpenMLDB.
    """

    conn_name_attr = 'openmldb_conn_id'
    default_conn_name = 'openmldb_default'
    conn_type = 'openmldb'
    hook_name = 'OpenMLDB'

    def __init__(self, openmldb_conn_id: str = "openmldb_default") -> None:
        super().__init__(http_conn_id=openmldb_conn_id)

    def submit_job(self, db: str, mode: str, sql: str):
        """
        Submits a job to a OpenMLDB API server.

        :param db: Required. The database in the OpenMLDB. If DDL, the db can be non-existent.
        :param mode: Required. Mode: offsync, offasync, online. If DDL, choose any mode.
        :param sql: Required. The sql of the OpenMLDB job.
        """
        return self.run(
            endpoint=f"dbs/{db}",
            json={"mode": mode, "sql": sql},
            headers={"accept": "application/json"},
        )
