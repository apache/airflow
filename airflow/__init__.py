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

isort:skip_file
"""

# flake8: noqa: F401
# pylint: disable=wrong-import-position
import sys
from typing import Callable, Optional

from airflow import settings
from airflow import version

__version__ = version.version

__all__ = ['__version__', 'login', 'DAG']

settings.initialize()

login: Optional[Callable] = None

PY37 = sys.version_info >= (3, 7)


def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    if name == "DAG":
        from airflow.models.dag import DAG  # pylint: disable=redefined-outer-name
        return DAG
    if name == "AirflowException":
        from airflow.exceptions import AirflowException  # pylint: disable=redefined-outer-name
        return AirflowException
    raise AttributeError(f"module {__name__} has no attribute {name}")


# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.dag import DAG
    from airflow.exceptions import AirflowException


if not PY37:
    from pep562 import Pep562

<<<<<<< HEAD
from airflow import configuration as conf

from airflow.models import DAG
from flask_admin import BaseView
from importlib import import_module
from airflow.exceptions import AirflowException

DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

login = None

_log = logging.getLogger(__name__)

def load_login():
    auth_backend = 'airflow.default_login'
    try:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            auth_backend = conf.get('webserver', 'auth_backend')
    except conf.AirflowConfigException:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            _log.warning(
                "auth_backend not found in webserver config reverting to "
                "*deprecated*  behavior of importing airflow_login")
            auth_backend = "airflow_login"

    try:
        global login
        login = import_module(auth_backend)
    except ImportError as err:
        _log.critical(
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
=======
    Pep562(__name__)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d
