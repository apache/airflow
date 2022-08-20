from typing import Any, Dict

import nzpy

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook


class NetezzaHook(DbApiHook):
    """
    General hook for jdbc db access.
    JDBC URL, username and password will be taken from the predefined connection.
    Note that the whole JDBC URL must be specified in the "host" field in the DB.
    Raises an airflow error if the given connection id doesn't exist.
    """

    conn_name_attr = 'nz_conn_id'
    default_conn_name = 'nz_default'
    conn_type = 'netezza'
    hook_name = 'Netezza Connection'
    supports_autocommit = True

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema'],
            "relabeling": {'extra': 'Database'},
        }

    def get_conn(self) -> nzpy.Connection:
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password
        port: int = conn.port
        db: str = conn.extra

        conn = nzpy.connect(user=login, password=psw, host=host, port=port, database=db)

        return conn

    def set_autocommit(self, conn: nzpy.Connection, autocommit: bool) -> None:
        """
        Enable or disable autocommit for the given connection.
        :param conn: The connection.
        :param autocommit: The connection's autocommit setting.
        """
        conn.autocommit = autocommit

    def get_autocommit(self, conn: nzpy.Connection) -> bool:
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False
        :param conn: The connection.
        :return: connection autocommit setting.
        :rtype: bool
        """
        return conn.autocommit
