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

from airflow.providers.postgres.hooks.postgres import PostgresHook

class RedshiftSqlHook(PostgresHook):
    """
    RedshiftSqlHook exposes the PostgresHook functionality to be used for Redshift
    .. seealso::
        :class:`~airflow.providers.postgres.hooks.postgres.PostgresHook`

    :param redshift_conn_id: The reference to a specific Redshift database.
    :type redshift_conn_id: str
    """

    conn_name_attr = 'redshift_conn_id'
    default_conn_name = 'redshift_default'
    conn_type = 'redshift'
    hook_name = 'RedshiftSql'
    supports_autocommit = True

    def __init__(self, redshift_conn_id: str = "redshift_default") -> None:
        super().__init__(postgres_conn_id=redshift_conn_id)