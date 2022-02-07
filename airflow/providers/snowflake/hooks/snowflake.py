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
import os
from contextlib import closing
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake import connector
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.util_text import split_statements
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from airflow.hooks.dbapi import DbApiHook
from airflow.utils.strings import to_boolean


def _try_to_boolean(value: Any):
    if isinstance(value, (str, type(None))):
        return to_boolean(value)
    return value


class SnowflakeHook(DbApiHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param insecure_mode: Turns off OCSP certificate checks.
        For details, see: `How To: Turn Off OCSP Checking in Snowflake Client Drivers - Snowflake Community
        <https://community.snowflake.com/s/article/How-to-turn-off-OCSP-checking-in-Snowflake-client-drivers>`__

    .. note::
        get_sqlalchemy_engine() depends on snowflake-sqlalchemy

    .. seealso::
        For more information on how to use this Snowflake connection, take a look at the guide:
        :ref:`howto/operator:SnowflakeOperator`
    """

    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    conn_type = 'snowflake'
    hook_name = 'Snowflake'
    supports_autocommit = True

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "extra__snowflake__account": StringField(lazy_gettext('Account'), widget=BS3TextFieldWidget()),
            "extra__snowflake__warehouse": StringField(
                lazy_gettext('Warehouse'), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__database": StringField(lazy_gettext('Database'), widget=BS3TextFieldWidget()),
            "extra__snowflake__region": StringField(lazy_gettext('Region'), widget=BS3TextFieldWidget()),
            "extra__snowflake__role": StringField(lazy_gettext('Role'), widget=BS3TextFieldWidget()),
            "extra__snowflake__insecure_mode": BooleanField(
                label=lazy_gettext('Insecure mode'), description="Turns off OCSP certificate checks"
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ['port'],
            "relabeling": {},
            "placeholders": {
                'extra': json.dumps(
                    {
                        "authenticator": "snowflake oauth",
                        "private_key_file": "private key",
                        "session_parameters": "session parameters",
                    },
                    indent=1,
                ),
                'schema': 'snowflake schema',
                'login': 'snowflake username',
                'password': 'snowflake password',
                'extra__snowflake__account': 'snowflake account name',
                'extra__snowflake__warehouse': 'snowflake warehouse name',
                'extra__snowflake__database': 'snowflake db name',
                'extra__snowflake__region': 'snowflake hosted region',
                'extra__snowflake__role': 'snowflake role',
                'extra__snowflake__insecure_mode': 'insecure mode',
            },
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)
        self.region = kwargs.pop("region", None)
        self.role = kwargs.pop("role", None)
        self.schema = kwargs.pop("schema", None)
        self.authenticator = kwargs.pop("authenticator", None)
        self.session_parameters = kwargs.pop("session_parameters", None)
        self.query_ids: List[str] = []

    def _get_conn_params(self) -> Dict[str, Optional[str]]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)  # type: ignore[attr-defined]
        account = conn.extra_dejson.get('extra__snowflake__account', '') or conn.extra_dejson.get(
            'account', ''
        )
        warehouse = conn.extra_dejson.get('extra__snowflake__warehouse', '') or conn.extra_dejson.get(
            'warehouse', ''
        )
        database = conn.extra_dejson.get('extra__snowflake__database', '') or conn.extra_dejson.get(
            'database', ''
        )
        region = conn.extra_dejson.get('extra__snowflake__region', '') or conn.extra_dejson.get('region', '')
        role = conn.extra_dejson.get('extra__snowflake__role', '') or conn.extra_dejson.get('role', '')
        schema = conn.schema or ''
        authenticator = conn.extra_dejson.get('authenticator', 'snowflake')
        session_parameters = conn.extra_dejson.get('session_parameters')
        insecure_mode = _try_to_boolean(
            conn.extra_dejson.get(
                'extra__snowflake__insecure_mode', conn.extra_dejson.get('insecure_mode', None)
            )
        )

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": self.schema or schema,
            "database": self.database or database,
            "account": self.account or account,
            "warehouse": self.warehouse or warehouse,
            "region": self.region or region,
            "role": self.role or role,
            "authenticator": self.authenticator or authenticator,
            "session_parameters": self.session_parameters or session_parameters,
            # application is used to track origin of the requests
            "application": os.environ.get("AIRFLOW_SNOWFLAKE_PARTNER", "AIRFLOW"),
        }
        if insecure_mode:
            conn_config['insecure_mode'] = insecure_mode

        # If private_key_file is specified in the extra json, load the contents of the file as a private
        # key and specify that in the connection configuration. The connection password then becomes the
        # passphrase for the private key. If your private key file is not encrypted (not recommended), then
        # leave the password empty.

        private_key_file = conn.extra_dejson.get('private_key_file')
        if private_key_file:
            with open(private_key_file, "rb") as key:
                passphrase = None
                if conn.password:
                    passphrase = conn.password.strip().encode()

                p_key = serialization.load_pem_private_key(
                    key.read(), password=passphrase, backend=default_backend()
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            conn_config['private_key'] = pkb
            conn_config.pop('password', None)

        return conn_config

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_params = self._get_conn_params()
        return self._conn_params_to_sqlalchemy_uri(conn_params)

    def _conn_params_to_sqlalchemy_uri(self, conn_params: Dict) -> str:
        return URL(
            **{
                k: v
                for k, v in conn_params.items()
                if v and k not in ['session_parameters', 'insecure_mode', 'private_key']
            }
        )

    def get_conn(self) -> SnowflakeConnection:
        """Returns a snowflake.connection object"""
        conn_config = self._get_conn_params()
        conn = connector.connect(**conn_config)
        return conn

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        engine_kwargs = engine_kwargs or {}
        conn_params = self._get_conn_params()
        if 'insecure_mode' in conn_params:
            engine_kwargs.setdefault('connect_args', dict())
            engine_kwargs['connect_args']['insecure_mode'] = True
        for key in ['session_parameters', 'private_key']:
            if conn_params.get(key):
                engine_kwargs.setdefault('connect_args', dict())
                engine_kwargs['connect_args'][key] = conn_params[key]
        return create_engine(self._conn_params_to_sqlalchemy_uri(conn_params), **engine_kwargs)

    def set_autocommit(self, conn, autocommit: Any) -> None:
        conn.autocommit(autocommit)
        conn.autocommit_mode = autocommit

    def get_autocommit(self, conn):
        return getattr(conn, 'autocommit_mode', False)

    def run(
        self,
        sql: Union[str, list],
        autocommit: bool = False,
        parameters: Optional[Union[dict, Iterable]] = None,
        handler: Optional[Callable] = None,
    ):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially. The variable execution_info is returned so that
        it can be used in the Operators to modify the behavior
        depending on the result of the query (i.e fail the operator
        if the copy has processed 0 files)

        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        """
        self.query_ids = []

        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)

            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with closing(conn.cursor(DictCursor)) as cur:

                for sql_statement in sql:

                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute(sql_statement, parameters)
                    else:
                        cur.execute(sql_statement)

                    execution_info = []
                    if handler is not None:
                        cur = handler(cur)
                    for row in cur:
                        self.log.info("Statement execution info - %s", row)
                        execution_info.append(row)

                    query_id = cur.sfqid
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)
                    self.query_ids.append(query_id)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

        return execution_info

    def test_connection(self):
        """Test the Snowflake connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
