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

from airflow.models import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

DEFAULT_DATE = datetime(2030, 1, 1)

# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(dag_id="test_zip_dag", start_date=DEFAULT_DATE, schedule="@daily")
dag1_task1 = EmptyOperator(task_id="dummy", dag=dag1, owner="airflow")

with DAG(dag_id="test_zip_autoregister", schedule=None, start_date=DEFAULT_DATE):
    EmptyOperator(task_id="noop")
