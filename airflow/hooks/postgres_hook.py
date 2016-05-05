import psycopg2
import psycopg2.extensions

from airflow.hooks.dbapi_hook import DbApiHook


class PostgresHook(DbApiHook):
    '''
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    '''
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = False

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema,
            port=conn.port)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'sslcrl']:
                conn_args[arg_name] = arg_val
        psycopg2_conn = psycopg2.connect(**conn_args)
        if psycopg2_conn.server_version < 70400:
            self.supports_autocommit = True
        return psycopg2_conn

    @staticmethod
    def _serialize_cell(cell):
        return psycopg2.extensions.adapt(cell).getquoted().decode('utf-8')
