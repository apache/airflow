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

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
# pylint: disable=no-name-in-module
from snowflake import connector

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
        super().__init__(*args, **kwargs)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)
        self.region = kwargs.pop("region", None)
        self.role = kwargs.pop("role", None)
        self.schema = kwargs.pop("schema", None)
        self.snowflake_conn_id = 'snowflake_conn_id'

    def _get_conn_params(self):
        """
        one method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)
        account = conn.extra_dejson.get('account', '')
        warehouse = conn.extra_dejson.get('warehouse', '')
        database = conn.extra_dejson.get('database', '')
        region = conn.extra_dejson.get("region", '')
        role = conn.extra_dejson.get('role', '')
        schema = conn.schema or ''

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": self.schema or schema,
            "database": self.database or database,
            "account": self.account or account,
            "warehouse": self.warehouse or warehouse,
            "region": self.region or region,
            "role": self.role or role

        }

        # If private_key_file is specified in the extra json, load the contents of the file as a private
        # key and specify that in the connection configuration. The connection password then becomes the
        # passphrase for the private key. If your private key file is not encrypted (not recommended), then
        # leave the password empty.

        private_key_file = conn.extra_dejson.get('private_key_file', None)
        if private_key_file:
            with open(private_key_file, "rb") as key:
                passphrase = None
                if conn.password:
                    passphrase = conn.password.strip().encode()

                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=passphrase,
                    backend=default_backend()
                )

            pkb = p_key.private_bytes(encoding=serialization.Encoding.DER,
                                      format=serialization.PrivateFormat.PKCS8,
                                      encryption_algorithm=serialization.NoEncryption())

            conn_config['private_key'] = pkb
            conn_config.pop('password', None)

        return conn_config

    def get_uri(self):
        """
        override DbApiHook get_uri method for get_sqlalchemy_engine()
        """
        conn_config = self._get_conn_params()
        uri = 'snowflake://{user}:{password}@{account}/{database}/{schema}' \
              '?warehouse={warehouse}&role={role}'
        return uri.format(**conn_config)

    def get_conn(self):
        """
        Returns a snowflake.connection object
        """
        conn_config = self._get_conn_params()
        conn = connector.connect(**conn_config)
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
