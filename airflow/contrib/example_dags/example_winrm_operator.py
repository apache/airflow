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
# --------------------------------------------------------------------------------
# Written By: Ekhtiar Syed
# Last Update: 8th April 2016
# Caveat: This Dag will not run because of missing scripts.
# The purpose of this is to give you a sample of a real world example DAG!
# --------------------------------------------------------------------------------

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import timedelta

from airflow.contrib.hooks.winrm_hook import WinRMHook
from airflow.contrib.operators.winrm_operator import WinRMOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='POC_winrm_parallel', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60))

cmd = 'ls -l'
run_this_last = DummyOperator(task_id='run_this_last', dag=dag)

winRMHook = WinRMHook(ssh_conn_id='ssh_POC1')

t1 = WinRMOperator(
    task_id="wintask1",
    command='ls -altr',
    winrm_hook=winRMHook,
    dag=dag)

t2 = WinRMOperator(
    task_id="wintask2",
    command='sleep 60',
    winrm_hook=winRMHook,
    dag=dag)

t3 = WinRMOperator(
    task_id="wintask3",
    command='echo \'luke test\' ',
    winrm_hook=winRMHook,
    dag=dag)

t1.set_downstream(run_this_last)
t2.set_downstream(run_this_last)
t3.set_downstream(run_this_last)
