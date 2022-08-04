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
from typing import Union

from airflow.models.base import ID_LEN, Base
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.connection import Connection
from airflow.models.dag import DAG, DagModel, DagTag
from airflow.models.dagbag import DagBag
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning
from airflow.models.dataset import Dataset
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ImportError
from airflow.models.log import Log
from airflow.models.mappedoperator import MappedOperator
from airflow.models.operator import Operator
from airflow.models.param import Param
from airflow.models.pool import Pool
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.skipmixin import SkipMixin
from airflow.models.slamiss import SlaMiss
from airflow.models.taskfail import TaskFail
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger
from airflow.models.variable import Variable
from airflow.models.xcom import XCOM_RETURN_KEY, XCom

__all__ = [
    "DAG",
    "ID_LEN",
    "XCOM_RETURN_KEY",
    "Base",
    "BaseOperator",
    "BaseOperatorLink",
    "Connection",
    "DagBag",
    "DagWarning",
    "DagModel",
    "DagPickle",
    "DagRun",
    "DagTag",
    "Dataset",
    "DbCallbackRequest",
    "ImportError",
    "Log",
    "MappedOperator",
    "Operator",
    "Param",
    "Pool",
    "RenderedTaskInstanceFields",
    "SkipMixin",
    "SlaMiss",
    "TaskFail",
    "TaskInstance",
    "TaskReschedule",
    "Trigger",
    "Variable",
    "XCom",
    "clear_task_instances",
]
