import cx_Oracle

from airflow.hooks.dbapi_hook import DbApiHook
from builtins import str
from past.builtins import basestring
from datetime import datetime
import numpy
import logging

class OracleHook(DbApiHook):
    """
    Interact with Oracle SQL.
    """
    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    supports_autocommit = False

    def get_conn(self):
        """
        Returns a oracle connection object
        Optional parameters for using a custom DSN connection (instead of using a server alias from tnsnames.ora)
        The dsn (data source name) is the TNS entry (from the Oracle names server or tnsnames.ora file) 
        or is a string like the one returned from makedsn().
        :param dsn: the host address for the Oracle server
        :param service_name: the db_unique_name of the database that you are connecting to (CONNECT_DATA part of TNS)
        You can set these parameters in the extra fields of your connection 
        as in ``{ "dsn":"some.host.address" , "service_name":"some.service.name" }``
        """
        conn = self.get_connection(self.oracle_conn_id)
        dsn = conn.extra_dejson.get('dsn', None)
        service_name = conn.extra_dejson.get('service_name', None)
        if dsn and service_name:            
            dsn = cx_Oracle.makedsn(dsn, conn.port, service_name=service_name)
            conn = cx_Oracle.connect(conn.login, conn.password, dsn=dsn)
        else:
            conn = cx_Oracle.connect(conn.login, conn.password, conn.host)
        return conn

    def insert_rows(self, table, rows, target_fields = None, commit_every = 1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        Changes from standard DbApiHook implementation:
        - Oracle SQL queries in cx_Oracle can not be terminated with a semicolon (';')
        - Replace NaN values with NULL using numpy.nan_to_num (not using is_nan() because of input types error for strings)
        - Coerce datetime cells to Oracle DATETIME format during insert
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = '({})'.format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        if self.supports_autocommit:
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
                elif type(cell) == float and numpy.isnan(cell): #coerce numpy NaN to NULL
                    l.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    l.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    l.append("to_date('" + cell.strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')")
                else:
                    l.append(str(cell))
            values = tuple(l)
            sql = 'INSERT /*+ APPEND */ INTO {0} {1} VALUES ({2})'.format(table, target_fields, ','.join(values))
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()
                logging.info('Loaded {i} into {table} rows so far'.format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info('Done loading. Loaded a total of {i} rows'.format(**locals()))
