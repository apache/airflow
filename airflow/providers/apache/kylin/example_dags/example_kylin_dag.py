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

"""
This is an example DAG which uses the KylinCubeOperator.
The tasks below include kylin build, refresh, merge operation.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.kylin.operators.kylin_cube import KylinCubeOperator

dag = DAG(
    dag_id='example_kylin_operator',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={'project': 'learn_kylin', 'cube': 'kylin_sales_cube'},
    tags=['example'],
)


@dag.task
def gen_build_time():
    """
    Gen build time and push to XCom (with key of "return_value")
    :return: A dict with build time values.
    """
    return {'date_start': '1325347200000', 'date_end': '1325433600000'}


gen_build_time_task = gen_build_time()
gen_build_time_output_date_start = gen_build_time_task['date_start']
gen_build_time_output_date_end = gen_build_time_task['date_end']

build_task1 = KylinCubeOperator(
    task_id="kylin_build_1",
    command='build',
    start_time=gen_build_time_output_date_start,
    end_time=gen_build_time_output_date_end,
    is_track_job=True,
    dag=dag,
)

build_task2 = KylinCubeOperator(
    task_id="kylin_build_2",
    command='build',
    start_time=gen_build_time_output_date_end,
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

refresh_task1 = KylinCubeOperator(
    task_id="kylin_refresh_1",
    command='refresh',
    start_time=gen_build_time_output_date_start,
    end_time=gen_build_time_output_date_end,
    is_track_job=True,
    dag=dag,
)

merge_task = KylinCubeOperator(
    task_id="kylin_merge",
    command='merge',
    start_time=gen_build_time_output_date_start,
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

disable_task = KylinCubeOperator(
    task_id="kylin_disable",
    command='disable',
    dag=dag,
)

purge_task = KylinCubeOperator(
    task_id="kylin_purge",
    command='purge',
    dag=dag,
)

build_task3 = KylinCubeOperator(
    task_id="kylin_build_3",
    command='build',
    start_time=gen_build_time_output_date_end,
    end_time='1328730000000',
    dag=dag,
)

build_task1 >> build_task2 >> refresh_task1 >> merge_task >> disable_task >> purge_task >> build_task3

# Task dependency created via `XComArgs`:
#   gen_build_time >> build_task1
#   gen_build_time >> build_task2
#   gen_build_time >> refresh_task1
#   gen_build_time >> merge_task
#   gen_build_time >> build_task3
