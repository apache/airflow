from pyhive import presto
from pyhive.exc import DatabaseError

from airflow.hooks.dbapi_hook import DbApiHook

import logging
logging.getLogger("pyhive").setLevel(logging.INFO)


class PrestoException(Exception):
    pass


class PrestoHook(DbApiHook):
    """
    Interact with Presto through PyHive!

    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = 'presto_conn_id'
    default_conn_name = 'presto_default'

    def get_conn(self):
        """Returns a connection object"""
        db = self.get_connection(self.presto_conn_id)
        return presto.connect(
            host=db.host,
            port=db.port,
            username=db.login,
            catalog=db.extra_dejson.get('catalog', 'hive'),
            schema=db.schema)

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters=None):
        """
        Get a set of records from Presto
        """
        try:
            return super(PrestoHook, self).get_records(
                self._strip_sql(hql), parameters)
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        try:
            return super(PrestoHook, self).get_first(
                self._strip_sql(hql), parameters)
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])

    def get_pandas_df(self, hql, parameters=None):
        """
        Get a pandas dataframe from a sql query.
        """
        import pandas
        cursor = self.get_cursor()
        cursor.execute(self._strip_sql(hql), parameters)
        try:
            data = cursor.fetchall()
        except DatabaseError as e:
            obj = eval(str(e))
            raise PrestoException(obj['message'])
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, hql, parameters=None):
        """
        Execute the statement against Presto. Can be used to create views.
        """
        return super(PrestoHook, self).run(self._strip_sql(hql), parameters)

    def insert_rows(self):
        raise NotImplemented()
