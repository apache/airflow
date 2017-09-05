# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

from airflow.exceptions import AirflowException
from airflow import configuration as conf
from importlib import import_module

from airflow.utils.log.LoggingMixin import LoggingMixin

api_auth = None

log = LoggingMixin().logger


def load_auth():
    auth_backend = 'airflow.api.auth.backend.default'
    try:
        auth_backend = conf.get("api", "auth_backend")
    except conf.AirflowConfigException:
        pass

    try:
        global api_auth
        api_auth = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import %s for API authentication due to: %s",
            auth_backend, err
        )
        raise AirflowException(err)
