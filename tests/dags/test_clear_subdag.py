# -*- coding: utf-8 -*-
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


import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator


def create_subdag_opt(main_dag):
    subdag_name = "daily_job"
    subdag = DAG(
        dag_id='.'.join([dag_name, subdag_name]),
        start_date=start_date,
        schedule_interval=None,
        concurrency=2,
    )
    BashOperator(
        bash_command="echo 1",
        task_id="daily_job_subdag_task",
        dag=subdag
    )
    return SubDagOperator(
        task_id=subdag_name,
        subdag=subdag,
        dag=main_dag,
    )


dag_name = "clear_subdag_test_dag"

start_date = datetime.datetime(2016, 1, 1)

dag = DAG(
    dag_id=dag_name,
    concurrency=3,
    start_date=start_date,
    schedule_interval="0 0 * * *"
)

daily_job_irrelevant = BashOperator(
    bash_command="echo 1",
    task_id="daily_job_irrelevant",
    dag=dag,
)

daily_job_downstream = BashOperator(
    bash_command="echo 1",
    task_id="daily_job_downstream",
    dag=dag,
)

daily_job = create_subdag_opt(main_dag=dag)

daily_job >> daily_job_downstream
