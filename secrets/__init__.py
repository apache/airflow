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
__all__ = ['BaseSecretsBackend', 'get_connections', 'get_variable', 'get_custom_secret_backend']

import json
from json import JSONDecodeError
from typing import TYPE_CHECKING, List, Optional

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.models.connection import Connection


CONFIG_SECTION = "secrets"
DEFAULT_SECRETS_SEARCH_PATH = [
    "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    "airflow.secrets.metastore.MetastoreBackend",
]


def get_connections(conn_id: str) -> List['Connection']:
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


def get_variable(key: str) -> Optional[str]:
    """
    Get Airflow Variable by iterating over all Secret Backends.

    :param key: Variable Key
    :return: Variable Value
    """
    for secrets_backend in ensure_secrets_loaded():
        var_val = secrets_backend.get_variable(key=key)
        if var_val is not None:
            return var_val

    return None


def get_custom_secret_backend() -> Optional[BaseSecretsBackend]:
    """Get Secret Backend if defined in airflow.cfg"""
    secrets_backend_cls = conf.getimport(section='secrets', key='backend')

    if secrets_backend_cls:
        try:
            alternative_secrets_config_dict = json.loads(
                conf.get(section=CONFIG_SECTION, key='backend_kwargs', fallback='{}')
            )
        except JSONDecodeError:
            alternative_secrets_config_dict = {}

        return secrets_backend_cls(**alternative_secrets_config_dict)
    return None


def initialize_secrets_backends() -> List[BaseSecretsBackend]:
    """
    * import secrets backend classes
    * instantiate them and return them in a list
    """
    backend_list = []

    custom_secret_backend = get_custom_secret_backend()

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

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
