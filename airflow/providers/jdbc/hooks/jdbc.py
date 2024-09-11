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

import traceback
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import jaydebeapi
from sqlalchemy.engine import URL

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection


@contextmanager
def suppress_and_warn(*exceptions: type[BaseException]):
    """Context manager that suppresses the given exceptions and logs a warning message."""
    try:
        yield
    except exceptions as e:
        warnings.warn(
            f"Exception suppressed: {e}\n{traceback.format_exc()}",
            category=UserWarning,
            stacklevel=3,
        )


class JdbcHook(DbApiHook):
    """
    General hook for JDBC access.

    JDBC URL, username and password will be taken from the predefined connection.
    Note that the whole JDBC URL must be specified in the "host" field in the DB.
    Raises an airflow error if the given connection id doesn't exist.

    To configure driver parameters, you can use the following methods:
        1. Supply them as constructor arguments when instantiating the hook.
        2. Set the "driver_path" and/or "driver_class" parameters in the "hook_params" dictionary when
           creating the hook using SQL operators.
        3. Set the "driver_path" and/or "driver_class" extra in the connection and correspondingly enable
           the "allow_driver_path_in_extra" and/or "allow_driver_class_in_extra" options in the
           "providers.jdbc" section of the Airflow configuration. If you're enabling these options in Airflow
           configuration, you should make sure that you trust the users who can edit connections in the UI
           to not use it maliciously.
        4. Define the "sqlalchemy_scheme" property in the extra of the connection if you want to use the
           SQLAlchemy engine from the JdbcHook.  When using the JdbcHook, the "sqlalchemy_scheme" will by
           default have the "jdbc" value, which is a protocol, not a database scheme or dialect.  So in order
           to be able to use SQLAlchemy with the JdbcHook, you need to define the "sqlalchemy_scheme"
           property in the extra of the connection.
        5. Patch the ``JdbcHook.default_driver_path`` and/or ``JdbcHook.default_driver_class`` values in the
           ``local_settings.py`` file.

    See :doc:`/connections/jdbc` for full documentation.

    :param args: passed to DbApiHook
    :param driver_path: path to the JDBC driver jar file. See above for more info
    :param driver_class: name of the JDBC driver class. See above for more info
    :param kwargs: passed to DbApiHook
    """

    conn_name_attr = "jdbc_conn_id"
    default_conn_name = "jdbc_default"
    conn_type = "jdbc"
    hook_name = "JDBC Connection"
    supports_autocommit = True

    default_driver_path: str | None = None
    default_driver_class: str | None = None

    def __init__(
        self,
        *args,
        driver_path: str | None = None,
        driver_class: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._driver_path = driver_path
        self._driver_class = driver_class

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Get custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"host": "Connection URL"},
        }

    @property
    def driver_path(self) -> str | None:
        from airflow.configuration import conf

        extra_driver_path = self.connection_extra_lower.get("driver_path")
        if extra_driver_path:
            if conf.getboolean("providers.jdbc", "allow_driver_path_in_extra", fallback=False):
                self._driver_path = extra_driver_path
            else:
                self.log.warning(
                    "You have supplied 'driver_path' via connection extra but it will not be used. In order "
                    "to use 'driver_path' from extra you must set airflow config setting "
                    "`allow_driver_path_in_extra = True` in section `providers.jdbc`. Alternatively you may "
                    "specify it via 'driver_path' parameter of the hook constructor or via 'hook_params' "
                    "dictionary with key 'driver_path' if using SQL operators."
                )
        if not self._driver_path:
            self._driver_path = self.default_driver_path
        return self._driver_path

    @property
    def driver_class(self) -> str | None:
        from airflow.configuration import conf

        extra_driver_class = self.connection_extra_lower.get("driver_class")
        if extra_driver_class:
            if conf.getboolean("providers.jdbc", "allow_driver_class_in_extra", fallback=False):
                self._driver_class = extra_driver_class
            else:
                self.log.warning(
                    "You have supplied 'driver_class' via connection extra but it will not be used. In order "
                    "to use 'driver_class' from extra you must set airflow config setting "
                    "`allow_driver_class_in_extra = True` in section `providers.jdbc`. Alternatively you may "
                    "specify it via 'driver_class' parameter of the hook constructor or via 'hook_params' "
                    "dictionary with key 'driver_class' if using SQL operators."
                )
        if not self._driver_class:
            self._driver_class = self.default_driver_class
        return self._driver_class

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        sqlalchemy_scheme = conn.extra_dejson.get("sqlalchemy_scheme")
        if sqlalchemy_scheme is None:
            raise AirflowException(
                "The parameter 'sqlalchemy_scheme' must be defined in extra for JDBC connections!"
            )
        return URL.create(
            drivername=sqlalchemy_scheme,
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=conn.schema,
        )

    def get_conn(self) -> jaydebeapi.Connection:
        conn: Connection = self.get_connection(self.get_conn_id())
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password

        conn = jaydebeapi.connect(
            jclassname=self.driver_class,
            url=str(host),
            driver_args=[str(login), str(psw)],
            jars=self.driver_path.split(",") if self.driver_path else None,
        )
        return conn

    def set_autocommit(self, conn: jaydebeapi.Connection, autocommit: bool) -> None:
        """
        Set autocommit for the given connection.

        :param conn: The connection.
        :param autocommit: The connection's autocommit setting.
        """
        with suppress_and_warn(jaydebeapi.Error):
            conn.jconn.setAutoCommit(autocommit)

    def get_autocommit(self, conn: jaydebeapi.Connection) -> bool:
        """
        Get autocommit setting for the provided connection.

        :param conn: Connection to get autocommit setting from.
        :return: connection autocommit setting. True if ``autocommit`` is set
            to True on the connection. False if it is either not set, set to
            False, or the connection does not support auto-commit.
        """
        with suppress_and_warn(jaydebeapi.Error):
            return conn.jconn.getAutoCommit()
        return False
