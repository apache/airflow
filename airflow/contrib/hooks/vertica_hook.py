from vertica_python import connect

from airflow.hooks.dbapi_hook import DbApiHook

class VerticaHook(DbApiHook):
    '''
    Interact with Vertica.
    '''

    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns verticaql connection object
        """
        conn = self.get_connection(self.vertica_conn_id)
        conn_config = {
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
        }

        conn_config["host"] = conn.host or 'localhost'
        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)

        conn = connect(**conn_config)
        return conn
