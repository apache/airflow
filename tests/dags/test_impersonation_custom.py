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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
# AIRFLOW-1893 - Originally, impersonation tests were incomplete missing the use case when
# DAGs access custom packages usually made available through the PYTHONPATH environment
# variable. This file includes a DAG that imports a custom package made available and if
# run via the previous implementation of impersonation, will fail by not being able to
# import the custom package.
# This DAG is used to test that impersonation propagates the PYTHONPATH environment
# variable correctly.
from fake_datetime import FakeDatetime

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
    'run_as_user': 'airflow_test_user'
}

dag = DAG(dag_id='impersonation_with_custom_pkg', default_args=args)


def print_today():
    date_time = FakeDatetime.utcnow()
    print('Today is {}'.format(date_time.strftime('%Y-%m-%d')))


def check_hive_conf():
    from airflow.configuration import conf
    assert conf.get('hive', 'default_hive_mapred_queue') == 'airflow'


PythonOperator(
    python_callable=print_today,
    task_id='exec_python_fn',
    dag=dag)

PythonOperator(
    python_callable=check_hive_conf,
    task_id='exec_check_hive_conf_fn',
    dag=dag)
