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
from airflow.configuration import conf
from airflow.models import DAG
from flask.ext.admin import BaseView


DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

from airflow import default_login as login
if conf.getboolean('webserver', 'AUTHENTICATE'):
    try:
        # Environment specific login
        import airflow_login as login
    except ImportError:
        logging.error(
            "authenticate is set to True in airflow.cfg, "
            "but airflow_login failed to import")


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
