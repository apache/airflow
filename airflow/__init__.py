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
#

"""
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`
"""
from builtins import object
from airflow import version
__version__ = version.version

import logging
import sys

from airflow import configuration as conf
from airflow import settings
from airflow.models import DAG
from flask_admin import BaseView
from importlib import import_module
from airflow.exceptions import AirflowException

if settings.DAGS_FOLDER not in sys.path:
    sys.path.append(settings.DAGS_FOLDER)

login = None


def load_login():
    auth_backend = 'airflow.default_login'
    try:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            auth_backend = conf.get('webserver', 'auth_backend')
    except conf.AirflowConfigException:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            logging.warning(
                "auth_backend not found in webserver config reverting to "
                "*deprecated*  behavior of importing airflow_login")
            auth_backend = "airflow_login"

    try:
        global login
        login = import_module(auth_backend)
    except ImportError as err:
        logging.critical(
            "Cannot import authentication module %s. "
            "Please correct your authentication backend or disable authentication: %s",
            auth_backend, err
        )
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            raise AirflowException("Failed to import authentication backend")


class AirflowViewPlugin(BaseView):
    pass


class AirflowMacroPlugin(object):
    def __init__(self, namespace):
        self.namespace = namespace

from airflow import operators
from airflow import hooks
from airflow import executors
from airflow import macros
from airflow import contrib

operators._integrate_plugins()
hooks._integrate_plugins()
executors._integrate_plugins()
macros._integrate_plugins()
