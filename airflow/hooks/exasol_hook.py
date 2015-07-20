import logging
import random
import jaydebeapi

from airflow import settings
from airflow.models import Connection
from airflow.utils import AirflowException


class ExasolHook(object):
    """
    Interact with Exasol
    """

    def __init__(
            self, host=None, login=None,
            psw=None, db=None, port=None, exasol_conn_id=None):
        if not exasol_conn_id:
            self.host = host
            self.login = login
            self.psw = psw
            self.db = db
            self.port = port
        else:
            session = settings.Session()
            db = session.query(
                Connection).filter(
                    Connection.conn_id == exasol_conn_id)
            if db.count() == 0:
                raise AirflowException("The exasol_dbid you provided isn't defined")
            else:
                db = db.all()[0]
            self.host = db.host
            self.login = db.login
            self.psw = db.password
            self.db = db.schema
            self.port = db.port
            session.commit()
            session.close()

    def get_conn(self):
        conn = jaydebeapi.connect('com.exasol.jdbc.EXADriver',
                           ['jdbc:exa:'+str(self.host)+':'+str(self.port)+';schema='+str(self.db), str(self.login), str(self.psw)],
                           '/var/exasol/exajdbc.jar',)
        return conn

    def get_records(self, sql):
        '''
        Executes the sql and returns a set of records.
        '''
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_pandas_df(self, sql):
        '''
        Executes the sql and returns a pandas dataframe
        '''
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn)
        conn.close()
        return df

    def run(self, sql, autocommit=False):
        conn = self.get_conn()
        conn.jconn.autocommit = autocommit
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
