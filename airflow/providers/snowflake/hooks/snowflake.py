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
from __future__ import annotations

import os
from contextlib import closing, contextmanager
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, TypeVar, overload
from urllib.parse import urlparse

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake import connector
from snowflake.connector import DictCursor, SnowflakeConnection, util_text
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook, return_single_query_results
from airflow.providers.snowflake.utils.openlineage import fix_snowflake_sqlalchemy_uri
from airflow.utils.strings import to_boolean

T = TypeVar("T")
if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.sqlparser import DatabaseInfo


def _try_to_boolean(value: Any):
    if isinstance(value, (str, type(None))):
        return to_boolean(value)
    return value


class SnowflakeHook(DbApiHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake account, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        ``https://<your_okta_account_name>.okta.com`` to authenticate
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
        ``get_sqlalchemy_engine()`` depends on ``snowflake-sqlalchemy``

    .. seealso::
        For more information on how to use this Snowflake connection, take a look at the guide:
        :ref:`howto/operator:SnowflakeOperator`
    """

    conn_name_attr = "snowflake_conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "Snowflake"
    supports_autocommit = True
    _test_connection_sql = "select 1"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "account": StringField(lazy_gettext("Account"), widget=BS3TextFieldWidget()),
            "warehouse": StringField(lazy_gettext("Warehouse"), widget=BS3TextFieldWidget()),
            "database": StringField(lazy_gettext("Database"), widget=BS3TextFieldWidget()),
            "region": StringField(lazy_gettext("Region"), widget=BS3TextFieldWidget()),
            "role": StringField(lazy_gettext("Role"), widget=BS3TextFieldWidget()),
            "private_key_file": StringField(lazy_gettext("Private key (Path)"), widget=BS3TextFieldWidget()),
            "private_key_content": StringField(
                lazy_gettext("Private key (Text)"), widget=BS3TextAreaFieldWidget()
            ),
            "insecure_mode": BooleanField(
                label=lazy_gettext("Insecure mode"), description="Turns off OCSP certificate checks"
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        import json

        return {
            "hidden_fields": ["port", "host"],
            "relabeling": {},
            "placeholders": {
                "extra": json.dumps(
                    {
                        "authenticator": "snowflake oauth",
                        "private_key_file": "private key",
                        "session_parameters": "session parameters",
                        "client_request_mfa_token": "client request mfa token",
                    },
                    indent=1,
                ),
                "schema": "snowflake schema",
                "login": "snowflake username",
                "password": "snowflake password",
                "account": "snowflake account name",
                "warehouse": "snowflake warehouse name",
                "database": "snowflake db name",
                "region": "snowflake hosted region",
                "role": "snowflake role",
                "private_key_file": "Path of snowflake private key (PEM Format)",
                "private_key_content": "Content to snowflake private key (PEM format)",
                "insecure_mode": "insecure mode",
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
        self.client_request_mfa_token = kwargs.pop("client_request_mfa_token", None)
        self.query_ids: list[str] = []

    def _get_field(self, extra_dict, field_name):
        backcompat_prefix = "extra__snowflake__"
        backcompat_key = f"{backcompat_prefix}{field_name}"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
                f"when using this method."
            )
        if field_name in extra_dict:
            import warnings

            if backcompat_key in extra_dict:
                warnings.warn(
                    f"Conflicting params `{field_name}` and `{backcompat_key}` found in extras. "
                    f"Using value for `{field_name}`.  Please ensure this is the correct "
                    f"value and remove the backcompat key `{backcompat_key}`.",
                    UserWarning,
                    stacklevel=2,
                )
            return extra_dict[field_name] or None
        return extra_dict.get(backcompat_key) or None

    @cached_property
    def _get_conn_params(self) -> dict[str, str | None]:
        """
        Fetch connection params as a dict.

        This is used in ``get_uri()`` and ``get_connection()``.
        """
        conn = self.get_connection(self.snowflake_conn_id)  # type: ignore[attr-defined]
        extra_dict = conn.extra_dejson
        account = self._get_field(extra_dict, "account") or ""
        warehouse = self._get_field(extra_dict, "warehouse") or ""
        database = self._get_field(extra_dict, "database") or ""
        region = self._get_field(extra_dict, "region") or ""
        role = self._get_field(extra_dict, "role") or ""
        insecure_mode = _try_to_boolean(self._get_field(extra_dict, "insecure_mode"))
        schema = conn.schema or ""
        client_request_mfa_token = _try_to_boolean(self._get_field(extra_dict, "client_request_mfa_token"))

        # authenticator and session_parameters never supported long name so we don't use _get_field
        authenticator = extra_dict.get("authenticator", "snowflake")
        session_parameters = extra_dict.get("session_parameters")

        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
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
            conn_config["insecure_mode"] = insecure_mode

        if client_request_mfa_token:
            conn_config["client_request_mfa_token"] = client_request_mfa_token

        # If private_key_file is specified in the extra json, load the contents of the file as a private key.
        # If private_key_content is specified in the extra json, use it as a private key.
        # As a next step, specify this private key in the connection configuration.
        # The connection password then becomes the passphrase for the private key.
        # If your private key is not encrypted (not recommended), then leave the password empty.

        private_key_file = self._get_field(extra_dict, "private_key_file")
        private_key_content = self._get_field(extra_dict, "private_key_content")

        private_key_pem = None
        if private_key_content and private_key_file:
            raise AirflowException(
                "The private_key_file and private_key_content extra fields are mutually exclusive. "
                "Please remove one."
            )
        elif private_key_file:
            private_key_file_path = Path(private_key_file)
            if not private_key_file_path.is_file() or private_key_file_path.stat().st_size == 0:
                raise ValueError("The private_key_file path points to an empty or invalid file.")
            if private_key_file_path.stat().st_size > 4096:
                raise ValueError("The private_key_file size is too big. Please keep it less than 4 KB.")
            private_key_pem = Path(private_key_file_path).read_bytes()
        elif private_key_content:
            private_key_pem = private_key_content.encode()

        if private_key_pem:
            passphrase = None
            if conn.password:
                passphrase = conn.password.strip().encode()

            p_key = serialization.load_pem_private_key(
                private_key_pem, password=passphrase, backend=default_backend()
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            conn_config["private_key"] = pkb
            conn_config.pop("password", None)

        refresh_token = self._get_field(extra_dict, "refresh_token") or ""
        if refresh_token:
            conn_config["refresh_token"] = refresh_token
            conn_config["authenticator"] = "oauth"
            conn_config["client_id"] = conn.login
            conn_config["client_secret"] = conn.password
            conn_config.pop("login", None)
            conn_config.pop("password", None)

        return conn_config

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()."""
        conn_params = self._get_conn_params
        return self._conn_params_to_sqlalchemy_uri(conn_params)

    def _conn_params_to_sqlalchemy_uri(self, conn_params: dict) -> str:
        return URL(
            **{
                k: v
                for k, v in conn_params.items()
                if v
                and k
                not in ["session_parameters", "insecure_mode", "private_key", "client_request_mfa_token"]
            }
        )

    def get_conn(self) -> SnowflakeConnection:
        """Return a snowflake.connection object."""
        conn_config = self._get_conn_params
        conn = connector.connect(**conn_config)
        return conn

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        engine_kwargs = engine_kwargs or {}
        conn_params = self._get_conn_params
        if "insecure_mode" in conn_params:
            engine_kwargs.setdefault("connect_args", {})
            engine_kwargs["connect_args"]["insecure_mode"] = True
        for key in ["session_parameters", "private_key"]:
            if conn_params.get(key):
                engine_kwargs.setdefault("connect_args", {})
                engine_kwargs["connect_args"][key] = conn_params[key]
        return create_engine(self._conn_params_to_sqlalchemy_uri(conn_params), **engine_kwargs)

    def get_snowpark_session(self):
        """
        Get a Snowpark session object.

        :return: the created session.
        """
        from snowflake.snowpark import Session

        from airflow import __version__ as airflow_version
        from airflow.providers.snowflake import __version__ as provider_version

        conn_config = self._get_conn_params
        session = Session.builder.configs(conn_config).create()
        # add query tag for observability
        session.update_query_tag(
            {
                "airflow_version": airflow_version,
                "airflow_provider_version": provider_version,
            }
        )
        return session

    def set_autocommit(self, conn, autocommit: Any) -> None:
        conn.autocommit(autocommit)
        conn.autocommit_mode = autocommit

    def get_autocommit(self, conn):
        return getattr(conn, "autocommit_mode", False)

    @overload  # type: ignore[override]
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
        return_dictionaries: bool = ...,
    ) -> None: ...

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: Callable[[Any], T] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
        return_dictionaries: bool = ...,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = True,
        return_last: bool = True,
        return_dictionaries: bool = False,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None:
        """
        Run a command or list of commands.

        Pass a list of SQL statements to the SQL parameter to get them to
        execute sequentially. The result of the queries is returned if the
        ``handler`` callable is set.

        :param sql: The SQL string to be executed with possibly multiple
            statements, or a list of sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of
            each statement.
        :param split_statements: Whether to split a single SQL string into
            statements and run separately
        :param return_last: Whether to return result for only last statement or
            for all after split.
        :param return_dictionaries: Whether to return dictionaries rather than
            regular DBAPI sequences as rows in the result. The dictionaries are
            of form ``{ 'column1_name': value1, 'column2_name': value2 ... }``.
        :return: Result of the last SQL statement if *handler* is set.
            *None* otherwise.
        """
        self.query_ids = []

        if isinstance(sql, str):
            if split_statements:
                split_statements_tuple = util_text.split_statements(StringIO(sql))
                sql_list: Iterable[str] = [
                    sql_string for sql_string, _ in split_statements_tuple if sql_string
                ]
            else:
                sql_list = [self.strip_sql_string(sql)]
        else:
            sql_list = sql

        if sql_list:
            self.log.debug("Executing following statements against Snowflake DB: %s", sql_list)
        else:
            raise ValueError("List of SQL statements is empty")

        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)

            with self._get_cursor(conn, return_dictionaries) as cur:
                results = []
                for sql_statement in sql_list:
                    self._run_command(cur, sql_statement, parameters)  # type: ignore[attr-defined]

                    if handler is not None:
                        result = self._make_common_data_structure(handler(cur))  # type: ignore[attr-defined]
                        if return_single_query_results(sql, return_last, split_statements):
                            _last_result = result
                            _last_description = cur.description
                        else:
                            results.append(result)
                            self.descriptions.append(cur.description)

                    query_id = cur.sfqid
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)
                    self.query_ids.append(query_id)

            # If autocommit was set to False or db does not support autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

        if handler is None:
            return None
        if return_single_query_results(sql, return_last, split_statements):
            self.descriptions = [_last_description]
            return _last_result
        else:
            return results

    @contextmanager
    def _get_cursor(self, conn: Any, return_dictionaries: bool):
        cursor = None
        try:
            if return_dictionaries:
                cursor = conn.cursor(DictCursor)
            else:
                cursor = conn.cursor()
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()

    def get_openlineage_database_info(self, connection) -> DatabaseInfo:
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        database = self.database or self._get_field(connection.extra_dejson, "database")

        return DatabaseInfo(
            scheme=self.get_openlineage_database_dialect(connection),
            authority=self._get_openlineage_authority(connection),
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "table_catalog",
            ],
            database=database,
            is_information_schema_cross_db=True,
            is_uppercase_names=True,
        )

    def get_openlineage_database_dialect(self, _) -> str:
        return "snowflake"

    def get_openlineage_default_schema(self) -> str | None:
        return self._get_conn_params["schema"]

    def _get_openlineage_authority(self, _) -> str | None:
        uri = fix_snowflake_sqlalchemy_uri(self.get_uri())
        return urlparse(uri).hostname

    def get_openlineage_database_specific_lineage(self, _) -> OperatorLineage | None:
        from airflow.providers.common.compat.openlineage.facet import ExternalQueryRunFacet
        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if self.query_ids:
            self.log.debug("openlineage: getting connection to get database info")
            connection = self.get_connection(self.get_conn_id())
            namespace = SQLParser.create_namespace(self.get_openlineage_database_info(connection))
            return OperatorLineage(
                run_facets={
                    "externalQuery": ExternalQueryRunFacet(
                        externalQueryId=self.query_ids[0], source=namespace
                    )
                }
            )
        return None
