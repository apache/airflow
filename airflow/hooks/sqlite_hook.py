import sqlite3

from airflow.hooks.dbapi_hook import DbApiHook


class SqliteHook(DbApiHook):

    """
    Interact with SQLite.
    """

    conn_name_attr = 'sqlite_conn_id'
    default_conn_name = 'sqlite_default'
    supports_autocommit = False

    def get_conn(self):
        """
        Returns a sqlite connection object
        """
        conn = self.get_connection(self.sqlite_conn_id)
        conn = sqlite3.connect(conn.host)
        return conn
