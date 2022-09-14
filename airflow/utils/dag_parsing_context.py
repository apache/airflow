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

import os
from contextlib import contextmanager
from typing import NamedTuple


class AirflowParsingContext(NamedTuple):
    """
    Context of parsing for the DAG.

    If these values are not None, they will contain the specific DAG and Task ID that Airflow is requesting to
    execute. You can use these for optimizing dynamically generated DAG files.
    """

    dag_id: str | None
    task_id: str | None


_AIRFLOW_PARSING_CONTEXT_DAG_ID = "_AIRFLOW_PARSING_CONTEXT_DAG_ID"
_AIRFLOW_PARSING_CONTEXT_TASK_ID = "_AIRFLOW_PARSING_CONTEXT_TASK_ID"


@contextmanager
def _airflow_parsing_context_manager(dag_id: str | None = None, task_id: str | None = None):
    old_dag_id = os.environ.get(_AIRFLOW_PARSING_CONTEXT_DAG_ID)
    old_task_id = os.environ.get(_AIRFLOW_PARSING_CONTEXT_TASK_ID)
    if dag_id is not None:
        os.environ[_AIRFLOW_PARSING_CONTEXT_DAG_ID] = dag_id
    if task_id is not None:
        os.environ[_AIRFLOW_PARSING_CONTEXT_TASK_ID] = task_id
    yield
    if old_task_id is not None:
        os.environ[_AIRFLOW_PARSING_CONTEXT_TASK_ID] = old_task_id
    if old_dag_id is not None:
        os.environ[_AIRFLOW_PARSING_CONTEXT_DAG_ID] = old_dag_id


def get_parsing_context() -> AirflowParsingContext:
    """Return the current (DAG) parsing context info"""
    return AirflowParsingContext(
        dag_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_DAG_ID),
        task_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_TASK_ID),
    )
