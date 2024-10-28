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
"""This dag only runs some simple tasks to test Airflow's task execution."""

from __future__ import annotations

# [START dag]
import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - datetime.timedelta(0, 0, 0, 0, 0, 3)).replace(
    minute=0, second=0, microsecond=0
)
START_DATE = now_to_the_hour
DAG_NAME = "test_dag_v1"

dag = DAG(
    DAG_NAME,
    schedule="*/10 * * * *",
    default_args={"depends_on_past": True},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

run_this_1 = EmptyOperator(task_id="run_this_1", dag=dag)
run_this_2 = EmptyOperator(task_id="run_this_2", dag=dag)
run_this_2.set_upstream(run_this_1)
run_this_3 = EmptyOperator(task_id="run_this_3", dag=dag)
run_this_3.set_upstream(run_this_2)
# [END dag]
