from datetime import datetime
import numpy
import logging

import MySQLdb

from airflow.hooks.base_hook import BaseHook


class MySqlHook(BaseHook):
    '''
    Interact with MySQL.
    '''

    def __init__(
            self, mysql_conn_id='mysql_default'):
        self.mysql_conn_id = mysql_conn_id

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn = MySQLdb.connect(
            conn.host,
            conn.login,
            conn.password,
            conn.schema)
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

    def run(self, sql):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute('SET autocommit = 0')
        conn.commit()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                if isinstance(cell, basestring):
                    l.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    l.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    l.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    l.append("'" + cell.isoformat() + "'")
                else:
                    l.append(str(cell))
            values = tuple(l)
            sql = "INSERT INTO {0} {1} VALUES ({2});".format(
                table,
                target_fields,
                ",".join(values))
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))
