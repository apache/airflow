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

import logging
import sys

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor

try:
    from airflow.executors.celery_executor import CeleryExecutor
except:
    pass

from airflow.exceptions import AirflowException


def _integrate_plugins():
    """Integrate plugins to the context."""
    from airflow.plugins_manager import executors_modules
    for executors_module in executors_modules:
        sys.modules[executors_module.__name__] = executors_module
        globals()[executors_module._name] = executors_module

_EXECUTOR = configuration.get('core', 'EXECUTOR')

if _EXECUTOR == 'LocalExecutor':
    DEFAULT_EXECUTOR = LocalExecutor()
elif _EXECUTOR == 'CeleryExecutor':
    DEFAULT_EXECUTOR = CeleryExecutor()
elif _EXECUTOR == 'SequentialExecutor':
    DEFAULT_EXECUTOR = SequentialExecutor()
elif _EXECUTOR == 'MesosExecutor':
    from airflow.contrib.executors.mesos_executor import MesosExecutor
    DEFAULT_EXECUTOR = MesosExecutor()
else:
    # Loading plugins
    _integrate_plugins()
    if _EXECUTOR in globals():
        DEFAULT_EXECUTOR = globals()[_EXECUTOR]()
    else:
        raise AirflowException("Executor {0} not supported.".format(_EXECUTOR))

logging.info("Using executor " + _EXECUTOR)
