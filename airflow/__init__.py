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

isort:skip_file
"""

# flake8: noqa: F401
# pylint:disable=wrong-import-position
from typing import Callable, Optional

# noinspection PyUnresolvedReferences
from airflow import utils
from airflow import settings
from airflow import version
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DAG

__version__ = version.version

settings.initialize()

login = None  # type: Optional[Callable]

from airflow import executors
from airflow import hooks
from airflow import macros
from airflow import operators
from airflow import sensors


class AirflowMacroPlugin:
    # pylint:disable=missing-docstring
    def __init__(self, namespace):
        self.namespace = namespace


operators._integrate_plugins()  # pylint:disable=protected-access
sensors._integrate_plugins()  # pylint:disable=protected-access
hooks._integrate_plugins()  # pylint:disable=protected-access
executors._integrate_plugins()  # pylint:disable=protected-access
macros._integrate_plugins()  # pylint:disable=protected-access
