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
"""Airflow models"""
from airflow.models.base import ID_LEN, Base  # noqa: F401
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink  # noqa: F401
from airflow.models.connection import Connection  # noqa: F401
from airflow.models.dag import DAG, DagModel  # noqa: F401
from airflow.models.dagbag import DagBag  # noqa: F401
from airflow.models.dagpickle import DagPickle  # noqa: F401
from airflow.models.dagrun import DagRun  # noqa: F401
from airflow.models.errors import ImportError  # noqa: F401, pylint:disable=redefined-builtin
from airflow.models.kubernetes import KubeResourceVersion, KubeWorkerIdentifier  # noqa: F401
from airflow.models.log import Log  # noqa: F401
from airflow.models.pool import Pool  # noqa: F401
from airflow.models.skipmixin import SkipMixin  # noqa: F401
from airflow.models.slamiss import SlaMiss  # noqa: F401
from airflow.models.taskfail import TaskFail  # noqa: F401
from airflow.models.taskinstance import TaskInstance, clear_task_instances  # noqa: F401
from airflow.models.taskreschedule import TaskReschedule  # noqa: F401
from airflow.models.variable import Variable  # noqa: F401
from airflow.models.xcom import XCOM_RETURN_KEY, XCom  # noqa: F401

# Load SQLAlchemy models during package initialization
# Must be loaded after loading DAG model.
# noinspection PyUnresolvedReferences
import airflow.jobs  # noqa: F401 isort # isort:skip
