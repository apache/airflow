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

import logging
from datetime import datetime

from airflow import DAG
from airflow.sdk import task

logger = logging.getLogger("airflow.demo_dag")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}


@task
def task1():
    logger.info("task1 running")


@task
def task2():
    logger.info("task2 running")


@task
def task3():
    logger.info("task3 running")


@task
def task4():
    logger.info("task4 running")


@task
def task5():
    logger.info("task5 running")


with DAG(
    "demo_dag",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    t1 = task1()
    t2 = task2()
    t3 = task3()
    t4 = task4()
    t5 = task5()

    # task1 and task2 fan out in parallel, task3 joins on both,
    # then task4 and task5 fan out in parallel.
    [t1, t2] >> t3 >> [t4, t5]
