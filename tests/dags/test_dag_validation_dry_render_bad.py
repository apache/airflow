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


class DryRenderOperator(BaseOperator):
    # Correct tuple: args will be templated and validated at parse time by our guardrails.
    template_fields = ("args",)

    def __init__(self, *, args=None, **kwargs):
        super().__init__(**kwargs)
        self.args = args or []


with DAG(
    dag_id="dry_render_bad_dag",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
):
    broken = DryRenderOperator(
        task_id="broken",
        args=[
            "--start_date",
            "{{ 'not-a-date' }}",
            "--end_date",
            "{{ 'still-not-a-date' }}",
        ],
    )
