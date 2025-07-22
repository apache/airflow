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
"""Example DAG demonstrating the usage of the XComArgs."""

from __future__ import annotations

# [START example_xcomargs]
import logging

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

log = logging.getLogger(__name__)


@task
def generate_value():
    """Empty function"""
    return "Bring me a shrubbery!"


@task
def print_value(value, ts=None):
    """Empty function"""
    log.info("The knights of Ni say: %s (at %s)", value, ts)


with DAG(
    dag_id="example_xcom_args",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    print_value(generate_value())

with DAG(
    "example_xcom_args_with_operators",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag2:
    bash_op1 = BashOperator(task_id="c", bash_command="echo c")
    bash_op2 = BashOperator(task_id="d", bash_command="echo c")
    xcom_args_a = print_value("first!")
    xcom_args_b = print_value("second!")

    bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2
# [END example_xcomargs]
