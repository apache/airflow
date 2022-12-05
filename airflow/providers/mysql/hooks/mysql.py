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
"""This module allows to connect to a MySQL database."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Union

from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from mysql.connector.abstracts import MySQLConnectionAbstract
    from MySQLdb.connections import Connection as MySQLdbConnection

MySQLConnectionTypes = Union["MySQLdbConnection", "MySQLConnectionAbstract"]


class MySqlHook(DbApiHook):
    """
    Interact with MySQL.

    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``

    :param schema: The MySQL database schema to connect to.
    :param connection: The :ref:`MySQL connection id <howto/connection:mysql>` used for MySQL credentials.
    """

    conn_name_attr = "mysql_conn_id"
    default_conn_name = "mysql_default"
    conn_type = "mysql"
    hook_name = "MySQL"
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.connection = kwargs.pop("connection", None)

    def set_autocommit(self, conn: MySQLConnectionTypes, autocommit: bool) -> None:
        """
        Set *autocommit*.

        *mysqlclient* uses an *autocommit* method rather than an *autocommit*
        property, so we need to override this to support it.

        :param conn: connection to set autocommit setting
        :param autocommit: autocommit setting
        """
        if hasattr(conn.__class__, "autocommit") and isinstance(conn.__class__.autocommit, property):
            conn.autocommit = autocommit
        else:
            conn.autocommit(autocommit)

    def get_autocommit(self, conn: MySQLConnectionTypes) -> bool:
        """
        Whether *autocommit* is active.

        *mysqlclient* uses an *get_autocommit* method rather than an *autocommit*
        property, so we need to override this to support it.

        :param conn: connection to get autocommit setting from.
        :return: connection autocommit setting
        """
        if hasattr(conn.__class__, "autocommit") and isinstance(conn.__class__.autocommit, property):
            return conn.autocommit
        else:
            return conn.get_autocommit()

    def _get_conn_config_mysql_client(self, conn: Connection) -> dict:
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or "",
            "host": conn.host or "localhost",
            "db": self.schema or conn.schema or "",
        }

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn_config["passwd"], conn.port = self.get_iam_token(conn)
            conn_config["read_default_group"] = "enable-cleartext-plugin"

        conn_config["port"] = int(conn.port) if conn.port else 3306

        if conn.extra_dejson.get("charset", False):
            conn_config["charset"] = conn.extra_dejson["charset"]
            if conn_config["charset"].lower() in ("utf8", "utf-8"):
                conn_config["use_unicode"] = True
        if conn.extra_dejson.get("cursor", False):
            import MySQLdb.cursors

            if (conn.extra_dejson["cursor"]).lower() == "sscursor":
                conn_config["cursorclass"] = MySQLdb.cursors.SSCursor
            elif (conn.extra_dejson["cursor"]).lower() == "dictcursor":
                conn_config["cursorclass"] = MySQLdb.cursors.DictCursor
            elif (conn.extra_dejson["cursor"]).lower() == "ssdictcursor":
                conn_config["cursorclass"] = MySQLdb.cursors.SSDictCursor
        local_infile = conn.extra_dejson.get("local_infile", False)
        if conn.extra_dejson.get("ssl", False):
            # SSL parameter for MySQL has to be a dictionary and in case
            # of extra/dejson we can get string if extra is passed via
            # URL parameters
            dejson_ssl = conn.extra_dejson["ssl"]
            if isinstance(dejson_ssl, str):
                dejson_ssl = json.loads(dejson_ssl)
            conn_config["ssl"] = dejson_ssl
        if conn.extra_dejson.get("ssl_mode", False):
            conn_config["ssl_mode"] = conn.extra_dejson["ssl_mode"]
        if conn.extra_dejson.get("unix_socket"):
            conn_config["unix_socket"] = conn.extra_dejson["unix_socket"]
        if local_infile:
            conn_config["local_infile"] = 1
        return conn_config

    def _get_conn_config_mysql_connector_python(self, conn: Connection) -> dict:
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "host": conn.host or "localhost",
            "database": self.schema or conn.schema or "",
            "port": int(conn.port) if conn.port else 3306,
        }

        if conn.extra_dejson.get("allow_local_infile", False):
            conn_config["allow_local_infile"] = True
        # Ref: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        for key, value in conn.extra_dejson.items():
            if key.startswith("ssl_"):
                conn_config[key] = value

        return conn_config

    def get_conn(self) -> MySQLConnectionTypes:
        """
        Connection to a MySQL database.

        Establishes a connection to a mysql database
        by extracting the connection configuration from the Airflow connection.

        .. note:: By default it connects to the database via the mysqlclient library.
            But you can also choose the mysql-connector-python library which lets you connect through ssl
            without any further ssl parameters required.

        :return: a mysql connection object
        """
        conn = self.connection or self.get_connection(getattr(self, self.conn_name_attr))

        client_name = conn.extra_dejson.get("client", "mysqlclient")

        if client_name == "mysqlclient":
            import MySQLdb

            conn_config = self._get_conn_config_mysql_client(conn)
            return MySQLdb.connect(**conn_config)

        if client_name == "mysql-connector-python":
            import mysql.connector

            conn_config = self._get_conn_config_mysql_connector_python(conn)
            return mysql.connector.connect(**conn_config)

        raise ValueError("Unknown MySQL client name provided!")

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """
        )
        conn.commit()
        conn.close()

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dump a database table into a tab-delimited file."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT * INTO OUTFILE '{tmp_file}'
            FROM {table}
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _serialize_cell(cell: object, conn: Connection | None = None) -> Any:
        """
        Convert argument to a literal.

        The package MySQLdb converts an argument to a literal
        when passing those separately to execute. Hence, this method does nothing.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The same cell
        """
        return cell

    def get_iam_token(self, conn: Connection) -> tuple[str, int]:
        """
        Retrieve a temporary password to connect to MySQL.

        Uses AWSHook to retrieve a temporary password to connect to MySQL
        Port is required. If none is provided, default 3306 is used
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        aws_conn_id = conn.extra_dejson.get("aws_conn_id", "aws_default")
        aws_hook = AwsBaseHook(aws_conn_id, client_type="rds")
        if conn.port is None:
            port = 3306
        else:
            port = conn.port
        client = aws_hook.get_conn()
        token = client.generate_db_auth_token(conn.host, port, conn.login)
        return token, port

    def bulk_load_custom(
        self, table: str, tmp_file: str, duplicate_key_handling: str = "IGNORE", extra_options: str = ""
    ) -> None:
        """
        A more configurable way to load local data from a file into the database.

        .. warning:: According to the mysql docs using this function is a
            `security risk <https://dev.mysql.com/doc/refman/8.0/en/load-data-local.html>`_.
            If you want to use it anyway you can do so by setting a client-side + server-side option.
            This depends on the mysql client library used.

        :param table: The table were the file will be loaded into.
        :param tmp_file: The file (name) that contains the data.
        :param duplicate_key_handling: Specify what should happen to duplicate data.
            You can choose either `IGNORE` or `REPLACE`.

            .. seealso::
                https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
        :param extra_options: More sql options to specify exactly how to load the data.

            .. seealso:: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            {duplicate_key_handling}
            INTO TABLE {table}
            {extra_options}
            """
        )

        cursor.close()
        conn.commit()
        conn.close()
