# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import random
from typing import Iterable

from airflow.models.connection import Connection
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

CONN_ENV_PREFIX = 'AIRFLOW_CONN_'


class BaseHook(LoggingMixin):
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
    @provide_session
    def _get_connections_from_db(cls, conn_id, session=None):
        db = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .all()
        )
        session.expunge_all()
        if not db:
            raise AirflowException(
                "The conn_id `{0}` isn't defined".format(conn_id))
        return db

    @classmethod
    def _get_connection_from_env(cls, conn_id):
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        conn = None
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
        return conn

    @classmethod
    def get_connections(cls, conn_id):  # type: (str) -> Iterable[Connection]
        conn = cls._get_connection_from_env(conn_id)
        if conn:
            conns = [conn]
        else:
            conns = cls._get_connections_from_db(conn_id)
        return conns

    @classmethod
    def get_connection(cls, conn_id):  # type: (str) -> Connection
        conn = random.choice(list(cls.get_connections(conn_id)))
        if conn.host:
            log = LoggingMixin().log
            log.info("Using connection to: %s", conn.debug_info())
        return conn

    @classmethod
    def get_hook(cls, conn_id):  # type: (str) -> BaseHook
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
