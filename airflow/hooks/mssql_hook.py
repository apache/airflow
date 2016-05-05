import pymssql

from airflow.hooks.dbapi_hook import DbApiHook


class MsSqlHook(DbApiHook):
    '''
    Interact with Microsoft SQL Server.
    '''

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        conn = self.get_connection(self.mssql_conn_id)
        conn = pymssql.connect(
            conn.host,
            conn.login,
            conn.password,
            conn.schema)
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)
