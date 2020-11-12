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
import os
import yaml
from typing import Any, List

from airflow.models.connection import Connection
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

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
    def is_connection_permitted(cls, conn_id: str, dag_conns = None) -> bool:
        """
        Checks if the DAG has access to use the connection

        :param conn_id: connection id
        :param dag_conns: Dag connection rules. If not specified then it will be loaded from common paths
        :return: boolean
        """
        dag_conns_yaml = os.path.join(os.getenv('AIRFLOW__CORE__PLUGINS_FOLDER'), 'dag_connections.yml')
        if not dag_conns and os.path.isfile(dag_conns_yaml):
            try:
                with open(dag_conns_yaml) as f:
                    dag_conns = yaml.load(f, Loader=yaml.FullLoader)
            except:
                raise AirflowException(f'Invalid YAML format in DAG to connection spec at {dag_conns}')

        if dag_conns:
            permitted_dags = next((x for x in dag_conns if x.get('connection') == conn_id), {}).get('dags', [])
            if os.getenv('AIRFLOW_CTX_DAG_ID') not in permitted_dags:
                return False

        return True

    @classmethod
    def get_connections(cls, conn_id: str) -> List[Connection]:
        """
        Get all connections as an iterable.

        :param conn_id: connection id
        :return: array of connections
        """
        return Connection.get_connections_from_secrets(conn_id)

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        """
        Get random connection selected from all connections configured with this connection id.

        :param conn_id: connection id
        :return: connection
        """
        conn = random.choice(cls.get_connections(conn_id))
        if conn.host:
            log.info(
                "Using connection to: id: %s. Host: %s, Port: %s, Schema: %s, Login: %s, Password: %s, "
                "extra: %s",
                conn.conn_id,
                conn.host,
                conn.port,
                conn.schema,
                conn.login,
                "XXXXXXXX" if conn.password else None,
                "XXXXXXXX" if conn.extra_dejson else None,
            )

        if not cls.is_connection_permitted(conn_id):
            raise AirflowException(f'This DAG has no access to "{conn_id}" connection"')

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

    def get_conn(self) -> Any:
        """Returns connection for the hook."""
        raise NotImplementedError()
