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
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

import prestodb
from deprecated import deprecated
from prestodb.exceptions import DatabaseError
from prestodb.transaction import IsolationLevel

from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException, AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.presto.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.context import AIRFLOW_VAR_NAME_FORMAT_MAPPING, DEFAULT_FORMAT_PREFIX
else:
    from airflow.utils.operator_helpers import (  # type: ignore[no-redef, attr-defined]
        AIRFLOW_VAR_NAME_FORMAT_MAPPING,
        DEFAULT_FORMAT_PREFIX,
    )

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

    from airflow.models import Connection

T = TypeVar("T")


def generate_presto_client_info() -> str:
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


class PrestoException(Exception):
    """Presto exception."""


def _boolify(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == "false":
            return False
        if value.lower() == "true":
            return True
    return value


class PrestoHook(DbApiHook):
    """
    Interact with Presto through prestodb.

    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = "presto_conn_id"
    default_conn_name = "presto_default"
    conn_type = "presto"
    hook_name = "Presto"
    strip_semicolon = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._placeholder: str = "?"

    def get_conn(self) -> Connection:
        """Return a connection object."""
        db = self.get_connection(self.get_conn_id())
        extra = db.extra_dejson
        auth = None
        if db.password and extra.get("auth") == "kerberos":
            raise AirflowException("Kerberos authorization doesn't support password.")
        if db.password:
            auth = prestodb.auth.BasicAuthentication(db.login, db.password)
        elif extra.get("auth") == "kerberos":
            auth = prestodb.auth.KerberosAuthentication(
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

        http_headers = {"X-Presto-Client-Info": generate_presto_client_info()}
        presto_conn = prestodb.dbapi.connect(
            host=db.host,
            port=db.port,
            user=db.login,
            source=db.extra_dejson.get("source", "airflow"),
            http_headers=http_headers,
            http_scheme=db.extra_dejson.get("protocol", "http"),
            catalog=db.extra_dejson.get("catalog", "hive"),
            schema=db.schema,
            auth=auth,
            isolation_level=self.get_isolation_level(),
        )
        if extra.get("verify") is not None:
            # Unfortunately verify parameter is available via public API.
            # The PR is merged in the presto library, but has not been released.
            # See: https://github.com/prestosql/presto-python-client/pull/31
            presto_conn._http_session.verify = _boolify(extra["verify"])

        return presto_conn

    @property
    def sqlalchemy_url(self) -> URL:
        """Return a `sqlalchemy.engine.URL` object constructed from the connection."""
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "SQLAlchemy is not installed. Please install it with "
                "`pip install 'apache-airflow-providers-presto[sqlalchemy]'`."
            ) from err

        conn = self.get_connection(self.get_conn_id())
        extra = conn.extra_dejson or {}

        required_attrs = ["host", "login", "port"]
        for attr in required_attrs:
            if getattr(conn, attr) is None:
                raise ValueError(f"Presto connections error: '{attr}' is missing in the connection")
        # adding only when **kwargs are given by user
        query = {
            k: v
            for k, v in {
                "schema": conn.schema,
                "protocol": extra.get("protocol"),
                "source": extra.get("source"),
                "catalog": extra.get("catalog"),
            }.items()
            if v is not None
        }
        return URL.create(
            drivername="presto",
            username=conn.login,
            password=conn.password or "",
            host=str(conn.host),
            port=conn.port,
            database=extra.get("catalog"),
            query=query,
        )

    def get_uri(self) -> str:
        """Return a SQLAlchemy engine URL as a string."""
        return self.sqlalchemy_url.render_as_string(hide_password=False)

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
            raise ValueError(f"The sql in Presto Hook must be a string and is {sql}!")
        try:
            return super().get_records(self.strip_sql_string(sql), parameters)
        except DatabaseError as e:
            raise PrestoException(e)

    def get_first(
        self, sql: str | list[str] = "", parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any:
        if not isinstance(sql, str):
            raise ValueError(f"The sql in Presto Hook must be a string and is {sql}!")
        try:
            return super().get_first(self.strip_sql_string(sql), parameters)
        except DatabaseError as e:
            raise PrestoException(e)

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
            raise PrestoException(e)
        column_descriptions = cursor.description
        if data:
            df = pd.DataFrame(data, **kwargs)
            df.rename(columns={n: c[0] for n, c in zip(df.columns, column_descriptions)}, inplace=True)
        else:
            df = pd.DataFrame(**kwargs)
        return df

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
            raise PrestoException(e)
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
        Insert a set of tuples into a table.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        """
        if self.get_isolation_level() == IsolationLevel.AUTOCOMMIT:
            self.log.info(
                "Transactions are not enable in presto connection. "
                "Please use the isolation_level property to enable it. "
                "Falling back to insert all rows in one transaction."
            )
            commit_every = 0

        super().insert_rows(table, rows, target_fields, commit_every)

    @staticmethod
    def _serialize_cell(cell: Any, conn: Connection | None = None) -> Any:
        """
        Presto will adapt all execute() args internally, hence we return cell without any conversion.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell
