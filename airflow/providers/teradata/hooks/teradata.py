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
"""A Airflow Hook for interacting with Teradata SQL Server."""
from __future__ import annotations

from typing import Any, NamedTuple

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
import logging as log
import sqlalchemy
import teradatasql
from teradatasql import TeradataConnection


class TeradataConnectionParams(NamedTuple):
    """Information about Teradata SQL Database connection parameters."""
    
    # Specifies the database hostname.
    host: str | None
    
    # Specifies Teradata Database port number.
    dbs_port: int | None
    
    # Specifies the initial database to use after logon.
    database: str | None
    
    # Specifies the Teradata Database username.
    user: str | None
    
    # Specifies the Teradata Database password.
    password: str | None


class TeradataHook(DbApiHook):
    """General hook for interacting with Teradata SQL Database.

    This module contains basic APIs to connect to and interact with Teradata SQL Database. It uses teradatasql client
    internally as a database driver for connecting to Teradata database. Teradata DB Server URL, username, password
    and database name are fetched from the predefined connection correcponding to connection_id. It raises an airflow
    error if the given connection id doesn't exist.

    See :doc:` docs/apache-airflow-providers-teradata/connections/teradata.rst` for full documentation.
    
    :param args: passed to DbApiHook
    :param kwargs: passed to DbApiHook
    
    
    Usage Help:
    
    >>> tdh = TeradataHook()
    >>> sql = "SELECT top 1 _airbyte_ab_id from airbyte_td._airbyte_raw_Sales;"
    >>> tdh.get_records(sql)
    [[61ad1d63-3efd-4da4-9904-a4489cc3a520]]
    
    """

    # Override to provide the connection name.
    conn_name_attr = "teradata_conn_id"
    
    # Override to have a default connection id for a particular dbHook
    default_conn_name = "teradata_default"
    
    # Override if this db supports autocommit.
    # supports_autocommit = False
    
    # Override this for hook to have a custom name in the UI selection
    conn_type = "teradata"
    
    # Override hook name to give descriptive name for hook
    hook_name = "Teradata"
    
    # Override with the Teradata specific placeholder parameter string used for insert queries
    placeholder: str = "?"

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)


    def get_conn(self) -> TeradataConnection:
        """Creates and returns a Teradata Connection object using teradatasql client"""
        """
        Connection to a Teradata database.

        Establishes a connection to a Teradata SQL database by extracting the connection configuration from the Airflow connection.

        .. note:: By default it connects to the database via the teradatasql library.
            But you can also choose the mysql-connector-python library which lets you connect through ssl
            without any further ssl parameters required.

        :return: a mysql connection object
        """
        print("Returns a Teradata connection object using teradatasql client")
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        teradata_conn_config: dict = self._get_conn_config_teradatasql(conn=conn)
        teradata_conn = teradatasql.connect(**teradata_conn_config)
        print("inside _get_teradatasql_connection")
        return teradata_conn
    
    
    def _get_conn_config_teradatasql(self, conn: Connection) -> dict[str, Any]:
        """
         Returns set of config parameters required for connecting to Teradata SQL Database using teradatasql client
        """
        conn_config = {
            "host": conn.host or "localhost",
            "dbs_port": conn.port or "1025",
            "database": conn.schema or "",
            "user": conn.login or "dbc",
            "password": conn.password or "dbc",
        }
        return conn_config
       
    
    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
         Returns a connection object using sqlalchemy
        """
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        log.info("Returns a Teradata connection object using sqlalchemy")
        link = 'teradatasql://{username}:{password}@{hostname}'.format(
               username=conn.login, password=conn.password, hostname=conn.host)
        log.info("link: {}".format(link))
        connection = sqlalchemy.create_engine(link)
        return connection
    

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form for Teradata database"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "host": StringField(lazy_gettext("Database Server Host URL"), widget=BS3TextFieldWidget()),
            "schema": StringField(lazy_gettext("Schema"), widget=BS3TextFieldWidget()),
            "login": StringField(lazy_gettext("User"), widget=BS3TextFieldWidget()),
            "password": PasswordField(lazy_gettext("Password"), widget=BS3PasswordFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {
                "host": "Database Server URL",
                "schema": "Database Name",
                "login": "Username",
                "password": "Password"
            },
            "placeholders": {
                "extra": json.dumps({"example_parameter": "parameter"}, indent=4),
                "login": "dbc",
                "password": "dbc",
            },
        }

