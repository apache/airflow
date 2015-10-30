from builtins import object
__version__ = "1.5.2"

import os
import sys

from airflow.configuration import conf, AirflowConfigException
from airflow.models import DAG
from airflow.utils import AirflowException

from flask.ext.admin import BaseView
from airflow.utils import AirflowException

DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)


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
