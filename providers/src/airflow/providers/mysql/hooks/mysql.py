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
import logging
from typing import TYPE_CHECKING, Any, Union

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.sql import DbApiHook

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.models import Connection

    try:
        from mysql.connector.abstracts import MySQLConnectionAbstract
    except ModuleNotFoundError:
        logger.warning(
            "The package 'mysql-connector-python' is not installed. Import skipped"
        )
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

    You can also add "local_infile" parameter to determine whether local_infile feature of MySQL client is
    going to be enabled (it is disabled by default).

    :param schema: The MySQL database schema to connect to.
    :param connection: The :ref:`MySQL connection id <howto/connection:mysql>` used for MySQL credentials.
    :param local_infile: Boolean flag determining if local_infile should be used
    :param init_command: Initial command to issue to MySQL server upon connection
    """

    conn_name_attr = "mysql_conn_id"
    default_conn_name = "mysql_default"
    conn_type = "mysql"
    hook_name = "MySQL"
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.local_infile = kwargs.pop("local_infile", False)
        self.init_command = kwargs.pop("init_command", None)

    def set_autocommit(self, conn: MySQLConnectionTypes, autocommit: bool) -> None:
        """
        Set *autocommit*.

        *mysqlclient* uses an *autocommit* method rather than an *autocommit*
        property, so we need to override this to support it.

        :param conn: connection to set autocommit setting
        :param autocommit: autocommit setting
        """
        if hasattr(conn.__class__, "autocommit") and isinstance(
            conn.__class__.autocommit, property
        ):
            conn.autocommit = autocommit
        else:
            conn.autocommit(autocommit)  # type: ignore[operator]

    def get_autocommit(self, conn: MySQLConnectionTypes) -> bool:
        """
        Whether *autocommit* is active.

        *mysqlclient* uses an *get_autocommit* method rather than an *autocommit*
        property, so we need to override this to support it.

        :param conn: connection to get autocommit setting from.
        :return: connection autocommit setting
        """
        if hasattr(conn.__class__, "autocommit") and isinstance(
            conn.__class__.autocommit, property
        ):
            return conn.autocommit
        else:
            return conn.get_autocommit()  # type: ignore[union-attr]

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

            cursor_type = conn.extra_dejson.get("cursor", "").lower()
            # Dictionary mapping cursor types to their respective classes
            cursor_classes = {
                "sscursor": MySQLdb.cursors.SSCursor,
                "dictcursor": MySQLdb.cursors.DictCursor,
                "ssdictcursor": MySQLdb.cursors.SSDictCursor,
            }
            # Set the cursor class in the connection configuration based on the cursor type
            if cursor_type in cursor_classes:
                conn_config["cursorclass"] = cursor_classes[cursor_type]

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
        if self.local_infile:
            conn_config["local_infile"] = 1
        if self.init_command:
            conn_config["init_command"] = self.init_command
        return conn_config

    def _get_conn_config_mysql_connector_python(self, conn: Connection) -> dict:
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "host": conn.host or "localhost",
            "database": self.schema or conn.schema or "",
            "port": int(conn.port) if conn.port else 3306,
        }

        if self.local_infile:
            conn_config["allow_local_infile"] = True
        if self.init_command:
            conn_config["init_command"] = self.init_command
        # Ref: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        for key, value in conn.extra_dejson.items():
            if key.startswith("ssl_"):
                conn_config[key] = value

        return conn_config

    def get_conn(self) -> MySQLConnectionTypes:
        """
        Get connection to a MySQL database.

        Establishes a connection to a mysql database
        by extracting the connection configuration from the Airflow connection.

        .. note:: By default it connects to the database via the mysqlclient library.
            But you can also choose the mysql-connector-python library which lets you connect through ssl
            without any further ssl parameters required.

        :return: a mysql connection object
        """
        conn = self.connection or self.get_connection(self.get_conn_id())

        client_name = conn.extra_dejson.get("client", "mysqlclient")

        if client_name == "mysqlclient":
            import MySQLdb

            conn_config = self._get_conn_config_mysql_client(conn)
            return MySQLdb.connect(**conn_config)

        if client_name == "mysql-connector-python":
            try:
                import mysql.connector
            except ModuleNotFoundError:
                raise AirflowOptionalProviderFeatureException(
                    "The pip package 'mysql-connector-python' is not installed, therefore the connection "
                    "wasn't established. Please, consider using default driver or pip install the package "
                    "'mysql-connector-python'. Warning! It might cause dependency conflicts."
                )

            conn_config = self._get_conn_config_mysql_connector_python(conn)
            return mysql.connector.connect(**conn_config)

        raise ValueError("Unknown MySQL client name provided!")

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"LOAD DATA LOCAL INFILE %s INTO TABLE {table}",
            (tmp_file,),
        )
        conn.commit()
        conn.close()  # type: ignore[misc]

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dump a database table into a tab-delimited file."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT * INTO OUTFILE %s FROM {table}",
            (tmp_file,),
        )
        conn.commit()
        conn.close()  # type: ignore[misc]

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
        self,
        table: str,
        tmp_file: str,
        duplicate_key_handling: str = "IGNORE",
        extra_options: str = "",
    ) -> None:
        """
        Load local data from a file into the database in a more configurable way.

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
            f"LOAD DATA LOCAL INFILE %s %s INTO TABLE {table} %s",
            (tmp_file, duplicate_key_handling, extra_options),
        )

        cursor.close()
        conn.commit()
        conn.close()  # type: ignore[misc]

    def get_openlineage_database_info(self, connection):
        """Return MySQL specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        return DatabaseInfo(
            scheme=self.get_openlineage_database_dialect(connection),
            authority=DbApiHook.get_openlineage_authority_part(
                connection, default_port=3306
            ),
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "column_type",
            ],
            normalize_name_method=lambda name: name.upper(),
        )

    def get_openlineage_database_dialect(self, _):
        """Return database dialect."""
        return "mysql"

    def get_openlineage_default_schema(self):
        """MySQL has no concept of schema."""
        return None
