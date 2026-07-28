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
"""Dynamic-DAG file using ``get_parsing_context()`` to early-return.

Used by the regression test for the case where ``DAG.test()`` must not set
``_AIRFLOW_PARSING_CONTEXT_DAG_ID`` while re-walking the owning bundle, or it
would strip the trigger target out of the bag.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, get_parsing_context

DEFAULT_DATE = datetime(2024, 1, 1)

PARENT_ID = "test_dag_test_dynamic_trigger_parent"
TARGET_ID = "test_dag_test_dynamic_trigger_target"

current = get_parsing_context().dag_id

if current is None or current == PARENT_ID:
    with DAG(
        dag_id=PARENT_ID,
        schedule=None,
        start_date=DEFAULT_DATE,
        is_paused_upon_creation=False,
    ):
        TriggerDagRunOperator(task_id="trigger_target", trigger_dag_id=TARGET_ID)

if current is None or current == TARGET_ID:
    with DAG(
        dag_id=TARGET_ID,
        schedule=None,
        start_date=DEFAULT_DATE,
        is_paused_upon_creation=False,
    ):
        EmptyOperator(task_id="target_task")
