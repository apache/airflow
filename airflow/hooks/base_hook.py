from builtins import object
import logging
import random

from airflow import settings
from airflow.models import Connection
from airflow.utils import AirflowException


class BaseHook(object):
    """
    Abstract base class for hooks, hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.
    """
    def __init__(self, source):
        pass

    def get_connections(self, conn_id):
        session = settings.Session()
        db = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .all()
        )
        if not db:
            raise AirflowException(
                "The conn_id `{0}` isn't defined".format(conn_id))
        session.expunge_all()
        session.close()
        return db

    def get_connection(self, conn_id):
        conn = random.choice(self.get_connections(conn_id))
        if conn.host:
            logging.info("Using connection to: " + conn.host)
        return conn

    def get_conn(self):
        raise NotImplemented()

    def get_records(self, sql):
        raise NotImplemented()

    def get_pandas_df(self, sql):
        raise NotImplemented()

    def run(self, sql):
        raise NotImplemented()
