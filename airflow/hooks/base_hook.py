from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import object
import logging
import os
import random

from airflow import settings
from airflow.models import Connection
from airflow.exceptions import AirflowException

import airflow.configuration as conf

import json
import requests

base_url = "http://localhost:8080/api/v1/"

CONN_ENV_PREFIX = 'AIRFLOW_CONN_'


class BaseHook(object):
    """
    Abstract base class for hooks, hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.
    """

    def __init__(self, dag=None):
        if not dag:
            logging.warning("Hook initialized without a DAG context.")

        self.dag = dag

    def get_connections(self, conn_id):
        if not self.dag:
            dag_id = "none"
        else:
            dag_id = self.dag.dag_id

        resp = requests.get(base_url + "/get_connections/{}/{}".format(dag_id, conn_id))
        if not resp.ok:
            raise AirflowException(
                "The conn_id `{0}` isn't defined for dag_id `{1}`".format(conn_id, dag_id))

        json_data = json.loads(resp.content)

        dbs = []
        for data in json_data['data']:
            conn = Connection(conn_id=conn_id,
                              conn_type=data['conn_type'],
                              host=data['host'],
                              port=data['port'],
                              schema=data['schema'],
                              password=data['password'],
                              extra=data['extra'],
                              )
            dbs.append(conn)

        return dbs

    def get_connection(self, conn_id):
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        conn = None
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
        else:
            conn = random.choice(self.get_connections(conn_id))
        if conn.host:
            logging.info("Using connection to: " + conn.host)
        return conn

    def get_hook(self, conn_id):
        connection = self.get_connection(conn_id)
        return connection.get_hook()

    def get_conn(self):
        raise NotImplementedError()

    def get_records(self, sql):
        raise NotImplementedError()

    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def run(self, sql):
        raise NotImplementedError()
