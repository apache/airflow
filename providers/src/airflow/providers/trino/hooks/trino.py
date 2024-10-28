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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping, TypeVar

import trino
from trino.exceptions import DatabaseError
from trino.transaction import IsolationLevel

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.utils.helpers import exactly_one
from airflow.utils.operator_helpers import (
    AIRFLOW_VAR_NAME_FORMAT_MAPPING,
    DEFAULT_FORMAT_PREFIX,
)

if TYPE_CHECKING:
    from airflow.models import Connection

T = TypeVar("T")


def generate_trino_client_info() -> str:
    """Return json string with dag_id, task_id, execution_date and try_number."""
    context_var = {
        format_map["default"].replace(DEFAULT_FORMAT_PREFIX, ""): os.environ.get(
            format_map["env_var_format"], ""
        )
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }
    task_info = {
        "dag_id": context_var["dag_id"],
        "task_id": context_var["task_id"],
        "execution_date": context_var["execution_date"],
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
        elif value.lower() == "true":
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
    query_id = ""
    _test_connection_sql = "select 1"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._placeholder: str = "?"

    def get_conn(self) -> Connection:
        """Return a connection object."""
        db = self.get_connection(self.trino_conn_id)  # type: ignore[attr-defined]
        extra = db.extra_dejson
        auth = None
        user = db.login
        if db.password and extra.get("auth") in ("kerberos", "certs"):
            raise AirflowException(
                f"The {extra.get('auth')!r} authorization type doesn't support password."
            )
        elif db.password:
            auth = trino.auth.BasicAuthentication(db.login, db.password)  # type: ignore[attr-defined]
        elif extra.get("auth") == "jwt":
            if not exactly_one(
                jwt_file := "jwt__file" in extra, jwt_token := "jwt__token" in extra
            ):
                msg = (
                    "When auth set to 'jwt' then expected exactly one parameter 'jwt__file' or 'jwt__token'"
                    " in connection extra, but "
                )
                if jwt_file and jwt_token:
                    msg += "provided both."
                else:
                    msg += "none of them provided."
                raise ValueError(msg)
            elif jwt_file:
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
            auth = trino.auth.KerberosAuthentication(  # type: ignore[attr-defined]
                config=extra.get("kerberos__config", os.environ.get("KRB5_CONFIG")),
                service_name=extra.get("kerberos__service_name"),
                mutual_authentication=_boolify(
                    extra.get("kerberos__mutual_authentication", False)
                ),
                force_preemptive=_boolify(extra.get("kerberos__force_preemptive", False)),
                hostname_override=extra.get("kerberos__hostname_override"),
                sanitize_mutual_error_response=_boolify(
                    extra.get("kerberos__sanitize_mutual_error_response", True)
                ),
                principal=extra.get(
                    "kerberos__principal", conf.get("kerberos", "principal")
                ),
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
            # type: ignore[func-returns-value]
            isolation_level=self.get_isolation_level(),
            verify=_boolify(extra.get("verify", True)),
            session_properties=extra.get("session_properties") or None,
            client_tags=extra.get("client_tags") or None,
            timezone=extra.get("timezone") or None,
        )

        return trino_conn

    def get_isolation_level(self) -> Any:
        """Return an isolation level."""
        db = self.get_connection(self.trino_conn_id)  # type: ignore[attr-defined]
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
        self,
        sql: str | list[str] = "",
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> Any:
        if not isinstance(sql, str):
            raise ValueError(f"The sql in Trino Hook must be a string and is {sql}!")
        try:
            return super().get_first(self.strip_sql_string(sql), parameters)
        except DatabaseError as e:
            raise TrinoException(e)

    def get_pandas_df(
        self,
        sql: str = "",
        parameters: Iterable | Mapping[str, Any] | None = None,
        **kwargs,
    ):  # type: ignore[override]
        import pandas as pd

        cursor = self.get_cursor()
        try:
            cursor.execute(self.strip_sql_string(sql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise TrinoException(e)
        column_descriptions = cursor.description
        if data:
            df = pd.DataFrame(data, **kwargs)
            df.rename(
                columns={n: c[0] for n, c in zip(df.columns, column_descriptions)},
                inplace=True,
            )
        else:
            df = pd.DataFrame(**kwargs)
        return df

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
