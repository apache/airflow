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
# under the License.
#

"""
A DAG with subdag for testing purpose.
"""

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME = 'test_subdag_operator'

DEFAULT_TASK_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1),
    'max_active_runs': 1,
}


def subdag(parent_dag_name, child_dag_name, args):
    """
    Create a subdag.
    """
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )

    for i in range(2):
        DummyOperator(
            task_id='%s-task-%s' % (child_dag_name, i + 1),
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    default_args=DEFAULT_TASK_ARGS,
    schedule_interval=timedelta(minutes=1),
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', DEFAULT_TASK_ARGS),
        default_args=DEFAULT_TASK_ARGS,
    )

    some_other_task = DummyOperator(
        task_id='some-other-task',
    )

    start >> section_1 >> some_other_task  # pylint: disable=W0104
