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
# flake8: noqa: F401
# pylint: disable=redefined-outer-name, too-many-locals, too-many-return-statements, too-many-branches

import sys

__all__ = ['ID_LEN', 'Base', 'BaseJob', 'BaseOperator', 'BaseOperatorLink', 'Connection',
           'DAG', 'DagModel', 'DagTag', 'DagBag', 'DagPickle', 'DagRun',
           'ImportError', 'KubeResourceVersion', 'KubeWorkerIdentifier', 'Log',
           'Pool', 'SerializedDagModel', 'SkipMixin', 'SlaMiss', 'TaskFail', 'TaskInstance',
           'clear_task_instances', 'TaskReschedule', 'Variable', 'XCOM_RETURN_KEY',
           'XCom']

PY37 = sys.version_info >= (3, 7)

def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    if name == 'ID_LEN':
        from airflow.models.base import ID_LEN
        return ID_LEN
    if name == 'Base':
        from airflow.models.base import Base
        return Base
    if name == 'BaseJob':
        from airflow.jobs.base_job import BaseJob
        return BaseJob
    if name == "BaseOperator":
        from airflow.models.baseoperator import BaseOperator
        return BaseOperator
    if name == "BaseOperatorLink":
        from airflow.models.baseoperator import BaseOperatorLink
        return BaseOperatorLink
    if name == 'Connection':
        from airflow.models.connection import Connection
        return Connection
    if name == 'DAG':
        from airflow.models.dag import DAG
        return DAG
    if name == 'DagModel':
        from airflow.models.dag import DagModel
        return DagModel
    if name == 'DagTag':
        from airflow.models.dag import DagTag
        return DagTag
    if name == 'DagBag':
        from airflow.models.dagbag import DagBag
        return DagBag
    if name == 'DagPickle':
        from airflow.models.dagpickle import DagPickle
        return DagPickle
    if name == 'DagRun':
        from airflow.models.dagrun import DagRun
        return DagRun
    if name == 'ImportError':
        from airflow.models.errors import ImportError
        return ImportError
    if name == 'KubeResourceVersion':
        from airflow.models.kubernetes import KubeResourceVersion
        return KubeResourceVersion
    if name == 'KubeWorkerIdentifier':
        from airflow.models.kubernetes import KubeWorkerIdentifier
        return KubeWorkerIdentifier
    if name == 'Log':
        from airflow.models.log import Log
        return Log
    if name == 'Pool':
        from airflow.models.pool import Pool
        return Pool
    if name == 'SerializedDagModel':
        from airflow.models.serialized_dag import SerializedDagModel
        return SerializedDagModel
    if name == 'SkipMixin':
        from airflow.models.skipmixin import SkipMixin
        return SkipMixin
    if name == 'SlaMiss':
        from airflow.models.slamiss import SlaMiss
        return SlaMiss
    if name == 'TaskFail':
        from airflow.models.taskfail import TaskFail
        return TaskFail
    if name == 'TaskInstance':
        from airflow.models.taskinstance import TaskInstance
        return TaskInstance
    if name == 'clear_task_instances':
        from airflow.models.taskinstance import clear_task_instances
        return clear_task_instances
    if name == 'TaskReschedule':
        from airflow.models.taskreschedule import TaskReschedule
        return TaskReschedule
    if name == 'Variable':
        from airflow.models.variable import Variable
        return Variable
    if name == 'XCOM_RETURN_KEY':
        from airflow.models.xcom import XCOM_RETURN_KEY
        return XCOM_RETURN_KEY
    if name == 'XCom':
        from airflow.models.xcom import XCom
        return XCom
    raise AttributeError(f"module {__name__} has no attribute {name}")


# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.base import ID_LEN
    from airflow.models.base import Base
    from airflow.jobs.base_job import BaseJob
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.baseoperator import BaseOperatorLink
    from airflow.models.connection import Connection
    from airflow.models.dag import DAG
    from airflow.models.dag import DagModel
    from airflow.models.dag import DagTag
    from airflow.models.dagbag import DagBag
    from airflow.models.dagpickle import DagPickle
    from airflow.models.dagrun import DagRun
    from airflow.models.errors import ImportError   # pylint: disable=redefined-builtin
    from airflow.models.kubernetes import KubeResourceVersion
    from airflow.models.kubernetes import KubeWorkerIdentifier
    from airflow.models.log import Log
    from airflow.models.pool import Pool
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.skipmixin import SkipMixin
    from airflow.models.slamiss import SlaMiss
    from airflow.models.taskfail import TaskFail
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstance import clear_task_instances
    from airflow.models.taskreschedule import TaskReschedule
    from airflow.models.variable import Variable
    from airflow.models.xcom import XCOM_RETURN_KEY
    from airflow.models.xcom import XCom

if not PY37:
    from pep562 import Pep562

    Pep562(__name__)
