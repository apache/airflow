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
__all__ = ['BaseSecretsBackend', 'get_connections']

import json
from abc import ABC, abstractmethod
from json import JSONDecodeError
from typing import List

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.module_loading import import_string

CONFIG_SECTION = "secrets"
DEFAULT_SECRETS_SEARCH_PATH = [
    "airflow.secrets.environment_variables.EnvironmentVariablesSecretsBackend",
    "airflow.secrets.metastore.MetastoreSecretsBackend",
]


class BaseSecretsBackend(ABC):
    """
    Abstract base class to retrieve secrets given a conn_id and construct a Connection object
    """

    def __init__(self, **kwargs):
        pass

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
    for secrets_backend in ensure_secrets_loaded():
        conn_list = secrets_backend.get_connections(conn_id=conn_id)
        if conn_list:
            return list(conn_list)

    raise AirflowException("The conn_id `{0}` isn't defined".format(conn_id))


def initialize_secrets_backends() -> List[BaseSecretsBackend]:
    """
    * import secrets backend classes
    * instantiate them and return them in a list
    """
    alternative_secrets_backend = conf.get(section=CONFIG_SECTION, key='backend', fallback='')
    try:
        alternative_secrets_config_dict = json.loads(
            conf.get(section=CONFIG_SECTION, key='backend_kwargs', fallback='{}')
        )
    except JSONDecodeError:
        alternative_secrets_config_dict = {}

    backend_list = []

    if alternative_secrets_backend:
        secrets_backend_cls = import_string(alternative_secrets_backend)
        backend_list.append(secrets_backend_cls(**alternative_secrets_config_dict))

    for class_name in DEFAULT_SECRETS_SEARCH_PATH:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


def ensure_secrets_loaded() -> List[BaseSecretsBackend]:
    """
    Ensure that all secrets backends are loaded.
    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends
    if len(secrets_backend_list) == 2:
        return initialize_secrets_backends()
    return secrets_backend_list


secrets_backend_list = initialize_secrets_backends()
