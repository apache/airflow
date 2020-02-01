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
"""Authentication backend"""
import logging
from importlib import import_module

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException

log = logging.getLogger(__name__)


class ApiAuth:  # pylint: disable=too-few-public-methods
    """Class to keep module of Authentication API  """
    def __init__(self):
        self.api_auth = None


API_AUTH = ApiAuth()


def load_auth():
    """Loads authentication backend"""
    auth_backend = 'airflow.api.auth.backend.default'
    try:
        auth_backend = conf.get("api", "auth_backend")
    except AirflowConfigException:
        pass

    try:
        API_AUTH.api_auth = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import %s for API authentication due to: %s",
            auth_backend, err
        )
        raise AirflowException(err)
