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
Example DAG to demonstrate reading asset events annotated with extra information.

Also see examples in ``example_outlet_event_extra.py``.
"""

from __future__ import annotations

import datetime

from airflow.assets import Asset
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.standard.core.operators.bash import BashOperator

asset = Asset("s3://output/1.txt")

with DAG(
    dag_id="read_asset_event",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["consumes"],
):

    @task(inlets=[asset])
    def read_asset_event(*, inlet_events=None):
        for event in inlet_events[asset][:-2]:
            print(event.extra["hi"])

    read_asset_event()

with DAG(
    dag_id="read_asset_event_from_classic",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["consumes"],
):
    BashOperator(
        task_id="read_asset_event_from_classic",
        inlets=[asset],
        bash_command="echo '{{ inlet_events['s3://output/1.txt'][-1].extra | tojson }}'",
    )
