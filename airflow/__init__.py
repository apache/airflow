"""
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`
"""
from builtins import object
__version__ = "1.5.2"

import logging
import os
import sys

from airflow.configuration import conf, AirflowConfigException
from airflow.models import DAG
from airflow.utils import AirflowException

from flask.ext.admin import BaseView
from importlib import import_module
from airflow.utils import AirflowException

DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

auth_backend = 'airflow.default_login'
try:
    auth_backend = conf.get('webserver', 'auth_backend')
except AirflowConfigException:
    if conf.getboolean('webserver', 'AUTHENTICATE'):
        logging.warning("auth_backend not found in webserver config reverting to *deprecated*"
                        " behavior of importing airflow_login")
        auth_backend = "airflow_login"

try:
    login = import_module(auth_backend)
except ImportError:
    logging.critical(
        "Cannot import authentication module %s. "
        "Please correct your authentication backend or disable authentication",
        auth_backend
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

operators.integrate_plugins()
hooks.integrate_plugins()
macros.integrate_plugins()
