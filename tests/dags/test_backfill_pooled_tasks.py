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


"""
DAG designed to test what happens when a DAG with pooled tasks is run
by a BackfillJob.
Addresses issue #1225.
"""
from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(dag_id='test_backfill_pooled_task_dag')
task = DummyOperator(
    task_id='test_backfill_pooled_task',
    dag=dag,
    pool='test_backfill_pooled_task_pool',
    owner='airflow',
    start_date=datetime(2016, 2, 1))
