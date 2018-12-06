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

import snowflake.connector

from airflow.hooks.dbapi_hook import DbApiHook


class SnowflakeHook(DbApiHook):
    """
    Interact with Snowflake.

    get_sqlalchemy_engine() depends on snowflake-sqlalchemy

    """

    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)
        self.region = kwargs.pop("region", None)

    def _get_conn_params(self):
        """
        one method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)
        account = conn.extra_dejson.get('account', None)
        warehouse = conn.extra_dejson.get('warehouse', None)
        database = conn.extra_dejson.get('database', None)
        region = conn.extra_dejson.get("region", None)

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": conn.schema or '',
            "database": self.database or database or '',
            "account": self.account or account or '',
            "warehouse": self.warehouse or warehouse or '',
            "region": self.region or region or ''
        }
        return conn_config

    def get_uri(self):
        """
        override DbApiHook get_uri method for get_sqlalchemy_engine()
        """
        conn_config = self._get_conn_params()
        uri = 'snowflake://{user}:{password}@{account}/{database}/'
        uri += '{schema}?warehouse={warehouse}'
        return uri.format(
            **conn_config)

    def get_conn(self):
        """
        Returns a snowflake.connection object
        """
        conn_config = self._get_conn_params()
        conn = snowflake.connector.connect(**conn_config)
        return conn

    def _get_aws_credentials(self):
        """
        returns aws_access_key_id, aws_secret_access_key
        from extra

        intended to be used by external import and export statements
        """
        if self.snowflake_conn_id:
            connection_object = self.get_connection(self.snowflake_conn_id)
            if 'aws_secret_access_key' in connection_object.extra_dejson:
                aws_access_key_id = connection_object.extra_dejson.get(
                    'aws_access_key_id')
                aws_secret_access_key = connection_object.extra_dejson.get(
                    'aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)
