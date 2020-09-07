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
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kylin.operators.kylin_cube import KylinCubeOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_kylin_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)


def gen_build_time(**kwargs):
    """
    Gen build time and push to xcom
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    ti.xcom_push(key='date_start', value='1325347200000')
    ti.xcom_push(key='date_end', value='1325433600000')


gen_build_time_task = PythonOperator(python_callable=gen_build_time, task_id='gen_build_time', dag=dag)

build_task1 = KylinCubeOperator(
    task_id="kylin_build_1",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time="{{ task_instance.xcom_pull(task_ids='gen_build_time',key='date_start') }}",
    end_time="{{ task_instance.xcom_pull(task_ids='gen_build_time',key='date_end') }}",
    is_track_job=True,
    dag=dag,
)

build_task2 = KylinCubeOperator(
    task_id="kylin_build_2",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time='1325433600000',
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

refresh_task1 = KylinCubeOperator(
    task_id="kylin_refresh_1",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='refresh',
    start_time='1325347200000',
    end_time='1325433600000',
    is_track_job=True,
    dag=dag,
)

merge_task = KylinCubeOperator(
    task_id="kylin_merge",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='merge',
    start_time='1325347200000',
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

disable_task = KylinCubeOperator(
    task_id="kylin_disable",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='disable',
    dag=dag,
)

purge_task = KylinCubeOperator(
    task_id="kylin_purge",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='purge',
    dag=dag,
)

build_task3 = KylinCubeOperator(
    task_id="kylin_build_3",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time='1325433600000',
    end_time='1325520000000',
    dag=dag,
)

gen_build_time_task >> build_task1 >> build_task2 >> refresh_task1 >> merge_task
merge_task >> disable_task >> purge_task >> build_task3
