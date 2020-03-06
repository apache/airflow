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
"""
Creds framework provides means of getting connection objects from various sources, e.g. the following:
    * Environment variables
    * Metatsore database
    * AWS SSM Parameter store
"""
__all__ = ['CONN_ENV_PREFIX', 'BaseCredsBackend', 'get_connections']

from abc import ABC, abstractmethod
from typing import List

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.module_loading import import_string

CONN_ENV_PREFIX = "AIRFLOW_CONN_"


class BaseCredsBackend(ABC):
    """
    Abstract base class to provide connection objects given a conn_id
    """

    @abstractmethod
    def get_connections(self, conn_id) -> List[Connection]:
        """
        Get list of connection objects

        :param conn_id:
        :return:
        """


def get_connections(conn_id: str) -> List[Connection]:
    """
    Get all connections as an iterable.

    :param conn_id: connection id
    :return: array of connections
    """
    creds_backends = conf.getlist(
        section="core",
        key="creds_backend",
        fallback=', '.join(
            [
                "airflow.creds.environment_variables.EnvironmentVariablesCredsBackend",
                "airflow.creds.metastore.MetastoreCredsBackend",
            ]
        ),
    )
    for creds_backend_name in creds_backends:
        creds_backend_cls = import_string(creds_backend_name)
        conn_list = creds_backend_cls().get_connections(conn_id=conn_id)
        if conn_list:
            return list(conn_list)

    raise AirflowException("The conn_id `{0}` isn't defined".format(conn_id))
