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
import time

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.timezone import datetime


class DummyWithOnKill(DummyOperator):
    def execute(self, context):
        time.sleep(10)

    def on_kill(self):
        self.log.info("Executing on_kill")
        f = open("/tmp/airflow_on_kill", "w")
        f.write("ON_KILL_TEST")
        f.close()


# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(
    dag_id='test_on_kill',
    start_date=datetime(2015, 1, 1))
dag1_task1 = DummyWithOnKill(
    task_id='task1',
    dag=dag1,
    owner='airflow')
