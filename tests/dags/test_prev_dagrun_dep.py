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
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(start_date=DEFAULT_DATE, owner="airflow")

# DAG tests depends_on_past dependencies
dag_dop = DAG(
    dag_id="test_depends_on_past", schedule=timedelta(days=1), default_args=default_args
)
with dag_dop:
    dag_dop_task = EmptyOperator(
        task_id="test_dop_task",
        depends_on_past=True,
    )

# DAG tests wait_for_downstream dependencies
dag_wfd = DAG(
    dag_id="test_wait_for_downstream",
    schedule=timedelta(days=1),
    default_args=default_args,
)
with dag_wfd:
    dag_wfd_upstream = EmptyOperator(
        task_id="upstream_task",
        wait_for_downstream=True,
    )
    dag_wfd_downstream = EmptyOperator(
        task_id="downstream_task",
    )
    dag_wfd_upstream >> dag_wfd_downstream
