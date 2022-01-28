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

from typing import Any, Dict, Optional

import jaydebeapi

from airflow.hooks.dbapi import DbApiHook
from airflow.models.connection import Connection


class JdbcHook(DbApiHook):
    """
    General hook for jdbc db access.

    JDBC URL, username and password will be taken from the predefined connection.
    Note that the whole JDBC URL must be specified in the "host" field in the DB.
    Raises an airflow error if the given connection id doesn't exist.
    """

    conn_name_attr = 'jdbc_conn_id'
    default_conn_name = 'jdbc_default'
    conn_type = 'jdbc'
    hook_name = 'JDBC Connection'
    supports_autocommit = True

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__jdbc__drv_path": StringField(lazy_gettext('Driver Path'), widget=BS3TextFieldWidget()),
            "extra__jdbc__drv_clsname": StringField(
                lazy_gettext('Driver Class'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'schema', 'extra'],
            "relabeling": {'host': 'Connection URL'},
        }

    def get_conn(self) -> jaydebeapi.Connection:
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password
        jdbc_driver_loc: Optional[str] = conn.extra_dejson.get('extra__jdbc__drv_path')
        jdbc_driver_name: Optional[str] = conn.extra_dejson.get('extra__jdbc__drv_clsname')

        conn = jaydebeapi.connect(
            jclassname=jdbc_driver_name,
            url=str(host),
            driver_args=[str(login), str(psw)],
            jars=jdbc_driver_loc.split(",") if jdbc_driver_loc else None,
        )
        return conn

    def set_autocommit(self, conn: jaydebeapi.Connection, autocommit: bool) -> None:
        """
        Enable or disable autocommit for the given connection.

        :param conn: The connection.
        :param autocommit: The connection's autocommit setting.
        """
        conn.jconn.setAutoCommit(autocommit)

    def get_autocommit(self, conn: jaydebeapi.Connection) -> bool:
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False

        :param conn: The connection.
        :return: connection autocommit setting.
        :rtype: bool
        """
        return conn.jconn.getAutoCommit()
