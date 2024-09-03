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

"""
Example DAG to demonstrate reading dataset events annotated with extra information.

Also see examples in ``example_outlet_event_extra.py``.
"""

from __future__ import annotations

import datetime

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

ds = Dataset("s3://output/1.txt")

with DAG(
    dag_id="read_dataset_event",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["consumes"],
):

    @task(inlets=[ds])
    def read_dataset_event(*, inlet_events=None):
        for event in inlet_events[ds][:-2]:
            print(event.extra["hi"])

    read_dataset_event()

with DAG(
    dag_id="read_dataset_event_from_classic",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["consumes"],
):
    BashOperator(
        task_id="read_dataset_event_from_classic",
        inlets=[ds],
        bash_command="echo '{{ inlet_events['s3://output/1.txt'][-1].extra | tojson }}'",
    )
