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
from airflow.operators.dummy import DummyOperator
from airflow.utils.context import Context
from airflow.utils.timezone import datetime


class DummyWithOnKill(DummyOperator):
    def execute(self, context: Context):
        import os

        self.log.info("Signalling that I am running")
        # signal to the test that we've started
        with open("/tmp/airflow_on_kill_running", "w") as f:
            f.write("ON_KILL_RUNNING")
        self.log.info("Signalled")

        # This runs extra processes, so that we can be sure that we correctly
        # tidy up all processes launched by a task when killing
        if not os.fork():
            os.system('sleep 10')
        time.sleep(10)

    def on_kill(self):
        self.log.info("Executing on_kill")
        with open("/tmp/airflow_on_kill_killed", "w") as f:
            f.write("ON_KILL_TEST")
        self.log.info("Executed on_kill")


class DummyWithOnKillWithContext(DummyOperator):
    def execute(self, context):
        import os

        # This runs extra processes, so that we can be sure that we correctly
        # tidy up all processes launched by a task when killing
        if not os.fork():
            os.system('sleep 10')
        time.sleep(10)

    def on_kill(self, context):
        self.log.info("Executing on_kill")
        with open("/tmp/airflow_on_kill", "w") as f:
            f.write(context['task'].task_id)


# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
with DAG(dag_id='test_on_kill', start_date=datetime(2015, 1, 1)) as dag1:
    dag1_task1 = DummyWithOnKill(task_id='task1', owner='airflow')

with DAG(dag_id='test_on_kill_with_context', start_date=datetime(2015, 1, 1)) as dag2:
    DummyWithOnKillWithContext(task_id='task2')
