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
from impala.dbapi import connect

from airflow.hooks.dbapi_hook import DbApiHook


class ImpalaHook(DbApiHook):
    """Interact with Apache Impala through impyla."""

    conn_name_attr = 'impala_conn_id'
    default_conn_name = 'impala_default'

    def get_conn(self):
        connection = self.get_connection(self.impala_conn_id)  # pylint: disable=no-member
        return connect(
            host=connection.host,
            port=connection.port,
            user=connection.login,
            password=connection.password,
            database=connection.schema,
        )
