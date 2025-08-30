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

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator


class FooOperator(BaseOperator):
    # Misconfigured on purpose: this is a string, not a tuple.
    # The warning is incidental; the failure is due to start/end date strings on the task.
    template_fields = "args"

    def __init__(self, args=None, **kwargs):
        super().__init__(**kwargs)
        self.args = args


class BarOperator(FooOperator):
    def __init__(self, start_date: str, end_date: str, **kwargs):
        super().__init__(
            args=["--start_date", start_date, "--end_date", end_date],
            **kwargs,
        )
        # The root bug: assign templated strings to BaseOperator fields -> remain strings at parse time
        self.start_date = start_date  # type: ignore[assignment]
        self.end_date = end_date  # type: ignore[assignment]


with DAG(
    dag_id="string_start_end_dates_dag",
    start_date=datetime(2025, 5, 1),
    schedule="0 7 * * *",
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    crash_task = BarOperator(
        task_id="crash_task",
        start_date="{{ (data_interval_start - macros.timedelta(days=11)).strftime('%Y-%m-%d') }}",
        end_date="{{ (data_interval_end - macros.timedelta(days=4)).strftime('%Y-%m-%d') }}",
    )
    end = EmptyOperator(task_id="end")
    start >> crash_task >> end
