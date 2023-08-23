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
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# [START howto_fine_grained_access_control]
with DAG(
    dag_id="example_fine_grained_access",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    access_control={
        "Viewer": {"can_edit", "can_create", "can_delete"},
    }
) as fine_grained_access:
# [END howto_fine_grained_access_control]
    
    task = EmptyOperator(
        task_id="run_this",
    )


# [START howto_no_fine_grained_access_control]
with DAG(
    dag_id="example_no_fine_grained_access",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    access_control={},
) as no_fine_grained_access:
# [END howto_no_fine_grained_access_control]
    
    task = EmptyOperator(
        task_id="run_this",
    )

# [START howto_indifferent_fine_grained_access_control]
with DAG(
    dag_id="example_indifferent_to_fine_grained_access",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    access_control={},
) as indifferent_to_fine_grained_access:
# [END howto_indifferent_fine_grained_access_control]
    
    task = EmptyOperator(
        task_id="run_this",
    )
