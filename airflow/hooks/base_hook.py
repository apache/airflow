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
"""Base class for all hooks"""
import logging
import random
from typing import List

from airflow import secrets
from airflow.models import Connection
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)


class BaseHook(LoggingMixin):
    """
    Abstract base class for hooks, hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.
    """

    @classmethod
    def get_connections(cls, conn_id: str) -> List[Connection]:
        """
        Get all connections as an iterable.

        :param conn_id: connection id
        :return: array of connections
        """
        return secrets.get_connections(conn_id)

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        """
        Get random connection selected from all connections configured with this connection id.

        :param conn_id: connection id
        :return: connection
        """
        conn = random.choice(list(cls.get_connections(conn_id)))
        if conn.host:
            log.info("Using connection to: %s", conn.log_info())
        return conn

    @classmethod
    def get_hook(cls, conn_id: str) -> "BaseHook":
        """
        Returns default hook for this connection id.

        :param conn_id: connection id
        :return: default hook for this connection
        """
        # TODO: set method return type to BaseHook class when on 3.7+.
        #  See https://stackoverflow.com/a/33533514/3066428
        connection = cls.get_connection(conn_id)
        return connection.get_hook()

    def get_conn(self):
        """Returns connection for the hook."""
        raise NotImplementedError()

    def get_records(self, sql):
        """Returns records for the sql query (for hooks that support SQL)."""
        # TODO: move it out from the base hook. It belongs to some common SQL hook most likely
        raise NotImplementedError()

    def get_pandas_df(self, sql):
        """Returns pandas dataframe for the sql query (for hooks that support SQL)."""
        # TODO: move it out from the base hook. It belongs to some common SQL hook most likely
        raise NotImplementedError()

    def run(self, sql):
        """Runs SQL query (for hooks that support SQL)."""
        # TODO: move it out from the base hook. It belongs to some common SQL hook most likely
        raise NotImplementedError()
