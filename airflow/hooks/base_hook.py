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

import json
import requests

base_url = "http://localhost:8080/api/v1"

CONN_ENV_PREFIX = 'AIRFLOW_CONN_'


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

    @classmethod
    def get_connections(cls, conn_id):
        resp = requests.get(base_url + "/get_connections/" + conn_id)
        if not resp.ok:
            raise AirflowException(
                "The conn_id `{0}` isn't defined".format(conn_id))

        json_data = json.loads(resp.content)
        logging.info("json data len {}".format(len(json_data['data'])))
        data = random.choice(json_data['data'])

        dbs = []
        for db in json_data['data']:
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

    @classmethod
    def get_connection(cls, conn_id):
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        conn = None
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
        else:
            conn = random.choice(cls.get_connections(conn_id))
        if conn.host:
            logging.info("Using connection to: " + conn.host)
        return conn

    @classmethod
    def get_hook(cls, conn_id):
        connection = cls.get_connection(conn_id)
        return connection.get_hook()

    def get_conn(self):
        raise NotImplementedError()

    def get_records(self, sql):
        raise NotImplementedError()

    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def run(self, sql):
        raise NotImplementedError()
