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
from __future__ import annotations

from datetime import datetime

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.informatica.lineage.selective import (
    disable_informatica_lineage,
    enable_informatica_lineage,
    is_task_auto_lineage_disabled,
)
from airflow.providers.standard.operators.empty import EmptyOperator


class _TaskWithPlainParams:
    def __init__(self):
        self.params = {}


def test_disable_and_enable_for_plain_dict_params():
    task = _TaskWithPlainParams()

    disable_informatica_lineage(task)
    assert is_task_auto_lineage_disabled(task) is True

    enable_informatica_lineage(task)
    assert is_task_auto_lineage_disabled(task) is False


def test_is_task_auto_lineage_disabled_returns_false_when_params_missing_or_none():
    class _NoParamsTask:
        pass

    class _NoneParamsTask:
        params = None

    assert is_task_auto_lineage_disabled(_NoParamsTask()) is False
    assert is_task_auto_lineage_disabled(_NoneParamsTask()) is False


def test_disable_and_enable_work_for_dag_tasks():
    dag = DAG(dag_id="test_selective_dag", start_date=datetime(2024, 1, 1), schedule=None)
    task_a = EmptyOperator(task_id="task_a", dag=dag)
    task_b = EmptyOperator(task_id="task_b", dag=dag)

    disable_informatica_lineage(dag)
    assert is_task_auto_lineage_disabled(task_a) is True
    assert is_task_auto_lineage_disabled(task_b) is True

    enable_informatica_lineage(dag)
    assert is_task_auto_lineage_disabled(task_a) is False
    assert is_task_auto_lineage_disabled(task_b) is False


def test_disable_with_xcomarg_returns_same_xcomarg_and_marks_underlying_task():
    dag = DAG(dag_id="test_selective_xcomarg", start_date=datetime(2024, 1, 1), schedule=None)
    task = EmptyOperator(task_id="task", dag=dag)

    xcomarg = task.output
    result = disable_informatica_lineage(xcomarg)

    assert result is xcomarg
    assert is_task_auto_lineage_disabled(task) is True
