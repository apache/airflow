# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
This sample "listen to directory". move the new file and print it, using docker-containers.
The following operators are being used: DockerOperator, BashOperator & ShortCircuitOperator.
TODO: Review the workflow, change it accordingly to to your environment & enable the code.
'''

# from __future__ import print_function
#
# from airflow import DAG
# import airflow
# from datetime import datetime, timedelta
# from airflow.operators import BashOperator
# from airflow.operators import ShortCircuitOperator
# from airflow.operators.docker_operator import DockerOperator
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now(),
#     'email': ['airflow@airflow.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# dag = DAG(
#         'docker_sample_copy_data', default_args=default_args, schedule_interval=timedelta(minutes=10))
#
# locate_file_cmd = """
#     sleep 10
#     find {{params.source_location}} -type f  -printf "%f\n" | head -1
# """
#
# t_view = BashOperator(
#         task_id='view_file',
#         bash_command=locate_file_cmd,
#         xcom_push=True,
#         params={'source_location': '/your/input_dir/path'},
#         dag=dag)
#
#
# def is_data_available(*args, **kwargs):
#     ti = kwargs['ti']
#     data = ti.xcom_pull(key=None, task_ids='view_file')
#     return not data == ''
#
#
# t_is_data_available = ShortCircuitOperator(
#         task_id='check_if_data_available',
#         provide_context=True,
#         python_callable=is_data_available,
#         dag=dag)
#
# t_move = DockerOperator(
#         api_version='1.19',
#         docker_url='tcp://localhost:2375',  # replace it with swarm/docker endpoint
#         image='centos:latest',
#         network_mode='bridge',
#         volumes=['/your/host/input_dir/path:/your/input_dir/path',
#                  '/your/host/output_dir/path:/your/output_dir/path'],
#         command='./entrypoint.sh',
#         task_id='move_data',
#         xcom_push=True,
#         params={'source_location': '/your/input_dir/path',
#                 'target_location': '/your/output_dir/path'},
#         dag=dag)
#
# print_templated_cmd = """
#     cat {{ ti.xcom_pull('move_data') }}
# """
#
# t_print = DockerOperator(
#         api_version='1.19',
#         docker_url='tcp://localhost:2375',
#         image='centos:latest',
#         volumes=['/your/host/output_dir/path:/your/output_dir/path'],
#         command=print_templated_cmd,
#         task_id='print',
#         dag=dag)
#
# t_view.set_downstream(t_is_data_available)
# t_is_data_available.set_downstream(t_move)
# t_move.set_downstream(t_print)
