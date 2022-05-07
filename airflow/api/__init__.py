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
"""Authentication backend"""
import logging
from importlib import import_module

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException

log = logging.getLogger(__name__)


def load_auth():
    """Loads authentication backends"""
    auth_backends = 'airflow.api.auth.backend.default'
    try:
        auth_backends = conf.get("api", "auth_backends")
    except AirflowConfigException:
        pass

    backends = []
    for backend in auth_backends.split(","):
        try:
            auth = import_module(backend.strip())
            log.info("Loaded API auth backend: %s", backend)
            backends.append(auth)
        except ImportError as err:
            log.critical("Cannot import %s for API authentication due to: %s", backend, err)
            raise AirflowException(err)
    return backends
