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
Secrets framework provides means of getting connection objects from various sources, e.g. the following:
    * Environment variables
    * Metatsore database
    * AWS SSM Parameter store
"""
__all__ = ['CONN_ENV_PREFIX', 'BaseSecretsBackend', 'get_connections']

import json
from abc import ABC, abstractmethod
from typing import List

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.module_loading import import_string

CONN_ENV_PREFIX = "AIRFLOW_CONN_"
CONFIG_SECTION = "secrets_backend"
CONFIG_KEY_CONFIG_JSON = 'config_json'
CONFIG_KEY_CLASS_NAME = 'class_name'

default_secrets_search_path = [
    "airflow.secrets.environment_variables.EnvironmentVariablesSecretsBackend",
    "airflow.secrets.metastore.MetastoreSecretsBackend",
]
secrets_backend = conf.get(section=CONFIG_SECTION, key=CONFIG_KEY_CLASS_NAME, fallback='',)
secrets_search_path = (
    [secrets_backend, *default_secrets_search_path]
    if secrets_backend
    else default_secrets_search_path
)


class BaseSecretsBackend(ABC):
    """
    Abstract base class to retrieve secrets given a conn_id and construct a Connection object
    """

    def __init__(self, *args, **kwargs):
        self._config_dict = None

    @property
    def config_dict(self):
        """
        Parse ``secrets_backend_config_json`` as dictionary.
        """
        if not self._config_dict:
            self._config_dict = json.loads(
                conf.get(section=CONFIG_SECTION, key=CONFIG_KEY_CONFIG_JSON)
            )
        return self._config_dict

    @abstractmethod
    def get_connections(self, conn_id) -> List[Connection]:
        """
        Return list of connection objects matching a given ``conn_id``.

        :param conn_id: connection id to search for
        :return:
        """


def get_connections(conn_id: str) -> List[Connection]:
    """
    Get all connections as an iterable.

    :param conn_id: connection id
    :return: array of connections
    """
    for class_name in secrets_search_path:
        secrets_backend_cls = import_string(class_name)
        conn_list = secrets_backend_cls().get_connections(conn_id=conn_id)
        if conn_list:
            return list(conn_list)

    raise AirflowException("The conn_id `{0}` isn't defined".format(conn_id))
