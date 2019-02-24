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
#

"""
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`
"""
from builtins import object
from airflow import version
from airflow.utils.log.logging_mixin import LoggingMixin

__version__ = version.version

import sys

# flake8: noqa: F401
from airflow import settings, configuration as conf
from airflow.models import DAG
from importlib import import_module
from airflow.exceptions import AirflowException

if settings.DAGS_FOLDER not in sys.path:
    sys.path.append(settings.DAGS_FOLDER)

login = None


def load_login():
    log = LoggingMixin().log

    auth_backend = 'airflow.default_login'
    try:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            auth_backend = conf.get('webserver', 'auth_backend')
    except conf.AirflowConfigException:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            log.warning(
                "auth_backend not found in webserver config reverting to "
                "*deprecated*  behavior of importing airflow_login")
            auth_backend = "airflow_login"

    try:
        global login
        login = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import authentication module %s. "
            "Please correct your authentication backend or disable authentication: %s",
            auth_backend, err
        )
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            raise AirflowException("Failed to import authentication backend")


class AirflowMacroPlugin(object):
    def __init__(self, namespace):
        self.namespace = namespace


from airflow import operators  # noqa: E402
from airflow import sensors  # noqa: E402
from airflow import hooks  # noqa: E402
from airflow import executors  # noqa: E402
from airflow import macros  # noqa: E402

operators._integrate_plugins()
sensors._integrate_plugins()  # noqa: E402
hooks._integrate_plugins()
executors._integrate_plugins()
macros._integrate_plugins()
