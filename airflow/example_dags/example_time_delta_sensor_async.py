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
Example DAG demonstrating ``TimeDeltaSensorAsync``, a drop in replacement for ``TimeDeltaSensor`` that
defers and doesn't occupy a worker slot while it waits
"""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.models.taskinstance import TaskNote
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="example_time_delta_sensor_async",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=datetime.timedelta(seconds=10))
    finish = EmptyOperator(task_id="finish")
    wait >> finish
#
#
# from uuid import uuid4
# import pendulum
# from airflow import DAG
# from airflow.models import DagModel, DagRun, TaskInstance as TI
# from airflow.models.taskinstance import TaskNote
# from airflow.operators.bash import BashOperator
# from airflow.settings import Session
#
# session = Session()
# run_id = str(uuid4())
#
# dm = DagModel(dag_id=run_id)
# dag = DAG(dag_id=run_id, start_date=pendulum.now())
# op = BashOperator(bash_command="", task_id="hi", dag=dag)
# dr = DagRun(dag_id=run_id, run_id=run_id, run_type="hi")
# ti = TI(task=op, run_id=run_id)
# session.add(dm)
# session.add(dr)
# session.add(ti)
# session.commit()
# ti.task_note is None
# ti.note is None
# ti.task_note = TaskNote("yo", None)
# ti.note = {"content": run_id, "user_id": None}
# session.commit()
# session.rollback()
# ti = session.query(TI).filter(TI.run_id == run_id).one()
# ti.note = {"content": "something else", "user_id": None}
# session.commit()
# ti.note = "this is wacky"
# session.commit()
# session.rollback()
