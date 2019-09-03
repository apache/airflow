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

from __future__ import print_function

from importlib import import_module
import warnings

import lazy_object_proxy
from zope.deprecation import deprecated

from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.configuration import conf

from airflow.utils.log.logging_mixin import LoggingMixin


class ApiAuth:  # pylint: disable=too-few-public-methods
    """Class to keep module of Authentication API  """
    def __init__(self):
        self.api_auth = None


API_AUTH = ApiAuth()

LOG = LoggingMixin().log


def load_auth():
    """Loads authentication backend"""

    auth_backend = 'airflow.api.auth.backend.default'
    try:
        auth_backend = conf.get("api", "auth_backend")
    except AirflowConfigException:
        pass

    try:
        api_auth = import_module(auth_backend)

        if api_auth is not API_AUTH.api_auth:
            # Only warn about this if the setting has changed

            if hasattr(api_auth, 'client_auth'):
                warnings.warn(
                    'Auth backend %s should provide a CLIENT_AUTH (instead of client_auth)' % auth_backend,
                    DeprecationWarning)
                api_auth.CLIENT_AUTH = api_auth.client_auth
            else:
                api_auth.client_auth = deprecated('use CLIENT_AUTH', api_auth.CLIENT_AUTH)
            API_AUTH.api_auth = api_auth
    except ImportError as err:
        LOG.critical(
            "Cannot import %s for API authentication due to: %s",
            auth_backend, err
        )
        raise AirflowException(err)


api_auth = lazy_object_proxy.Proxy(lambda: API_AUTH.api_auth)
deprecated('api_auth', 'Use airflow.api.API_AUTH.api_auth instead')
