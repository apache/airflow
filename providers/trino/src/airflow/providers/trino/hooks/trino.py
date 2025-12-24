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

import json
import os
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, overload
from urllib.parse import quote_plus, urlencode

import trino
from deprecated import deprecated
from trino.exceptions import DatabaseError
from trino.transaction import IsolationLevel

from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException, AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.trino.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.helpers import exactly_one

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.context import AIRFLOW_VAR_NAME_FORMAT_MAPPING, DEFAULT_FORMAT_PREFIX
else:
    from airflow.utils.operator_helpers import (  # type: ignore[no-redef, attr-defined]
        AIRFLOW_VAR_NAME_FORMAT_MAPPING,
        DEFAULT_FORMAT_PREFIX,
    )

if TYPE_CHECKING:
    from airflow.models import Connection

T = TypeVar("T")


def generate_trino_client_info() -> str:
    """Return json string with dag_id, task_id, logical_date and try_number."""
    context_var = {
        format_map["default"].replace(DEFAULT_FORMAT_PREFIX, ""): os.environ.get(
            format_map["env_var_format"], ""
        )
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }
    date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
    task_info = {
        "dag_id": context_var["dag_id"],
        "task_id": context_var["task_id"],
        date_key: context_var[date_key],
        "try_number": context_var["try_number"],
        "dag_run_id": context_var["dag_run_id"],
        "dag_owner": context_var["dag_owner"],
    }
    return json.dumps(task_info, sort_keys=True)


class TrinoException(Exception):
    """Trino exception."""


def _boolify(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == "false":
            return False
        if value.lower() == "true":
            return True
    return value


class TrinoHook(DbApiHook):
    """
    Interact with Trino through trino package.

    >>> ph = TrinoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = "trino_conn_id"
    default_conn_name = "trino_default"
    conn_type = "trino"
    hook_name = "Trino"
    strip_semicolon = True
    query_id = ""
    _test_connection_sql = "select 1"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": [],
            "relabeling": {},
            "placeholders": {
                "extra": json.dumps(
                    {
                        "auth": "authentication type",
                        "impersonate_as_owner": "allow impersonate as owner",
                        "jwt__token": "JWT token",
                        "jwt__file": "JWT file path",
                        "certs__client_cert_path": "Client certificate path",
                        "certs__client_key_path": "Client key path",
                        "kerberos__config": "Kerberos config",
                        "kerberos__service_name": "Kerberos service name",
                        "kerberos__mutual_authentication": "Kerberos mutual authentication",
                        "kerberos__force_preemptive": "Kerberos force preemptive",
                        "kerberos__hostname_override": "Kerberos hostname override",
                        "kerberos__sanitize_mutual_error_response": "Kerberos sanitize mutual error response",
                        "kerberos__principal": "Kerberos principal",
                        "kerberos__delegate": "Kerberos delegate",
                        "kerberos__ca_bundle": "Kerberos CA bundle",
                        "session_properties": "session properties",
                        "client_tags": "Trino client tags. Example ['sales','cluster1']",
                        "timezone": "Trino timezone",
                    },
                    indent=1,
                ),
                "login": "Effective user for connection",
            },
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._placeholder: str = "?"

    def get_conn(self) -> Connection:
        """Return a connection object."""
        db = self.get_connection(self.get_conn_id())
        extra = db.extra_dejson
        auth = None
        user = db.login
        auth_methods = []
        if db.password:
            auth_methods.append("password")
        if extra.get("auth") == "jwt":
            auth_methods.append("jwt")
        if extra.get("auth") == "certs":
            auth_methods.append("certs")
        if extra.get("auth") == "kerberos":
            auth_methods.append("kerberos")
        if len(auth_methods) > 1:
            raise AirflowException(
                f"Multiple authentication methods specified: {', '.join(auth_methods)}. Only one is allowed."
            )
        if db.password:
            auth = trino.auth.BasicAuthentication(db.login, db.password)
        elif extra.get("auth") == "jwt":
            if not exactly_one(jwt_file := "jwt__file" in extra, jwt_token := "jwt__token" in extra):
                msg = (
                    "When auth set to 'jwt' then expected exactly one parameter 'jwt__file' or 'jwt__token'"
                    " in connection extra, but "
                )
                if jwt_file and jwt_token:
                    msg += "provided both."
                else:
                    msg += "none of them provided."
                raise ValueError(msg)
            if jwt_file:
                token = Path(extra["jwt__file"]).read_text()
            else:
                token = extra["jwt__token"]
            auth = trino.auth.JWTAuthentication(token=token)
        elif extra.get("auth") == "certs":
            auth = trino.auth.CertificateAuthentication(
                extra.get("certs__client_cert_path"),
                extra.get("certs__client_key_path"),
            )
        elif extra.get("auth") == "kerberos":
            auth = trino.auth.KerberosAuthentication(
                config=extra.get("kerberos__config", os.environ.get("KRB5_CONFIG")),
                service_name=extra.get("kerberos__service_name"),
                mutual_authentication=_boolify(extra.get("kerberos__mutual_authentication", False)),
                force_preemptive=_boolify(extra.get("kerberos__force_preemptive", False)),
                hostname_override=extra.get("kerberos__hostname_override"),
                sanitize_mutual_error_response=_boolify(
                    extra.get("kerberos__sanitize_mutual_error_response", True)
                ),
                principal=extra.get("kerberos__principal", conf.get("kerberos", "principal")),
                delegate=_boolify(extra.get("kerberos__delegate", False)),
                ca_bundle=extra.get("kerberos__ca_bundle"),
            )

        if _boolify(extra.get("impersonate_as_owner", False)):
            user = os.getenv("AIRFLOW_CTX_DAG_OWNER", None)
            if user is None:
                user = db.login
        http_headers = {"X-Trino-Client-Info": generate_trino_client_info()}
        trino_conn = trino.dbapi.connect(
            host=db.host,
            port=db.port,
            user=user,
            source=extra.get("source", "airflow"),
            http_scheme=extra.get("protocol", "http"),
            http_headers=http_headers,
            catalog=extra.get("catalog", "hive"),
            schema=db.schema,
            auth=auth,
            isolation_level=self.get_isolation_level(),
            verify=_boolify(extra.get("verify", True)),
            session_properties=extra.get("session_properties") or None,
            client_tags=extra.get("client_tags") or None,
            timezone=extra.get("timezone") or None,
            extra_credential=extra.get("extra_credential") or None,
            roles=extra.get("roles") or None,
        )

        return trino_conn

    def get_isolation_level(self) -> Any:
        """Return an isolation level."""
        db = self.get_connection(self.get_conn_id())
        isolation_level = db.extra_dejson.get("isolation_level", "AUTOCOMMIT").upper()
        return getattr(IsolationLevel, isolation_level, IsolationLevel.AUTOCOMMIT)

    def get_records(
        self,
        sql: str | list[str] = "",
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> Any:
        if not isinstance(sql, str):
            raise ValueError(f"The sql in Trino Hook must be a string and is {sql}!")
        try:
            return super().get_records(self.strip_sql_string(sql), parameters)
        except DatabaseError as e:
            raise TrinoException(e)

    def get_first(
        self, sql: str | list[str] = "", parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any:
        if not isinstance(sql, str):
            raise ValueError(f"The sql in Trino Hook must be a string and is {sql}!")
        try:
            return super().get_first(self.strip_sql_string(sql), parameters)
        except DatabaseError as e:
            raise TrinoException(e)

    def _get_pandas_df(self, sql: str = "", parameters=None, **kwargs):
        try:
            import pandas as pd
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "Pandas is not installed. Please install it with `pip install pandas`."
            )

        cursor = self.get_cursor()
        try:
            cursor.execute(self.strip_sql_string(sql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise TrinoException(e)
        column_descriptions = cursor.description
        if data:
            df = pd.DataFrame(data, **kwargs)
            df.rename(columns={n: c[0] for n, c in zip(df.columns, column_descriptions)}, inplace=True)
        else:
            df = pd.DataFrame(**kwargs)
        return df

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
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
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = True,
        return_last: bool = True,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None:
        """
        Override common run to set split_statements=True by default.

        :param sql: SQL statement or list of statements to execute.
        :param autocommit: Set autocommit mode before query execution.
        :param parameters: Parameters to render the SQL query with.
        :param handler: Optional callable to process each statement result.
        :param split_statements: Split single SQL string into statements if True.
        :param return_last: Return only last statement result if True.
        :return: Query result or list of results.
        """
        return super().run(sql, autocommit, parameters, handler, split_statements, return_last)

    def _get_polars_df(self, sql: str = "", parameters=None, **kwargs):
        try:
            import polars as pl
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "Polars is not installed. Please install it with `pip install polars`."
            )

        cursor = self.get_cursor()
        try:
            cursor.execute(self.strip_sql_string(sql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise TrinoException(e)
        column_descriptions = cursor.description
        if data:
            df = pl.DataFrame(
                data,
                schema=[c[0] for c in column_descriptions],
                orient="row",
                **kwargs,
            )
        else:
            df = pl.DataFrame(**kwargs)
        return df

    @deprecated(
        reason="Replaced by function `get_df`.",
        category=AirflowProviderDeprecationWarning,
        action="ignore",
    )
    def get_pandas_df(self, sql: str = "", parameters=None, **kwargs):
        return self._get_pandas_df(sql, parameters, **kwargs)

    def insert_rows(
        self,
        table: str,
        rows: Iterable[tuple],
        target_fields: Iterable[str] | None = None,
        commit_every: int = 0,
        replace: bool = False,
        **kwargs,
    ) -> None:
        """
        Insert a set of tuples into a table in a generic way.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        """
        if self.get_isolation_level() == IsolationLevel.AUTOCOMMIT:
            self.log.info(
                "Transactions are not enable in trino connection. "
                "Please use the isolation_level property to enable it. "
                "Falling back to insert all rows in one transaction."
            )
            commit_every = 0

        super().insert_rows(table, rows, target_fields, commit_every, replace)

    @staticmethod
    def _serialize_cell(cell: Any, conn: Connection | None = None) -> Any:
        """
        Trino will adapt all execute() args internally, hence we return cell without any conversion.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell

    def get_openlineage_database_info(self, connection):
        """Return Trino specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        return DatabaseInfo(
            scheme="trino",
            authority=DbApiHook.get_openlineage_authority_part(
                connection, default_port=trino.constants.DEFAULT_PORT
            ),
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "table_catalog",
            ],
            database=connection.extra_dejson.get("catalog", "hive"),
            is_information_schema_cross_db=True,
        )

    def get_openlineage_database_dialect(self, _):
        """Return Trino dialect."""
        return "trino"

    def get_openlineage_default_schema(self):
        """Return Trino default schema."""
        return trino.constants.DEFAULT_SCHEMA

    def get_uri(self) -> str:
        """Return the Trino URI for the connection."""
        conn = self.connection
        uri = "trino://"

        auth_part = ""
        if conn.login:
            auth_part = quote_plus(conn.login)
            if conn.password:
                auth_part = f"{auth_part}:{quote_plus(conn.password)}"
            auth_part = f"{auth_part}@"

        host_part = conn.host or "localhost"
        if conn.port:
            host_part = f"{host_part}:{conn.port}"

        schema_part = ""
        if conn.schema:
            schema_part = f"/{quote_plus(conn.schema)}"
            extra_schema = conn.extra_dejson.get("schema")
            if extra_schema:
                schema_part = f"{schema_part}/{quote_plus(extra_schema)}"

        uri = f"{uri}{auth_part}{host_part}{schema_part}"

        extra = conn.extra_dejson.copy()
        if "schema" in extra:
            extra.pop("schema")

        query_params = {k: str(v) for k, v in extra.items() if v is not None}
        if query_params:
            uri = f"{uri}?{urlencode(query_params)}"

        return uri
