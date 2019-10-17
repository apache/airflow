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
This example illustrates the use of the ClearTaskOperator.

This example illustrates the following features:
1. When ClearTaskOperator runs, it clears the state of the given external_task_id.
2. If downstream is True (the default), all downstream tasks of the external task are also cleared
3. If future is True, all future instances of the external task are also cleared.
"""
import datetime

from airflow.contrib.operators.clear_task_operator import ClearTaskOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

dag = DAG(dag_id="example_clear_task_operator",
          default_args={"owner": "airflow"},
          schedule_interval=None,
          start_date=datetime.datetime(2019, 1, 1))

dummy1 = DummyOperator(task_id="dummy1", dag=dag)
dummy2 = DummyOperator(task_id="dummy2", dag=dag)
dummy1_sensor = ExternalTaskSensor(task_id="dummy1_sensor", external_task_id=dummy1.task_id,
                                   external_dag_id=dag.dag_id)
clear_dummy1 = ClearTaskOperator(task_id="clear_dummy1", external_task_id=dummy1.task_id,
                                 downstream=False, dag=dag)

dummy1 >> dummy2
dummy1_sensor >> clear_dummy1

clear_dummy1_downstream = ClearTaskOperator(task_id="clear_dummy1_downstream",
                                            external_task_id=dummy1.task_id,
                                            downstream=True, dag=dag)
dummy1_sensor_downstream = ExternalTaskSensor(task_id="dummy1_sensor_downstream",
                                              external_task_id=dummy1.task_id,
                                              external_dag_id=dag.dag_id)

clear_dummy1 >> dummy1_sensor_downstream >> clear_dummy1_downstream

clear_dummy1_future = ClearTaskOperator(task_id="clear_dummy1_future",
                                        external_task_id=dummy1.task_id,
                                        downstream=False, future=True, dag=dag)
dummy2_sensor_future = ExternalTaskSensor(task_id="dummy2_sensor_future",
                                          external_task_id=dummy2.task_id,
                                          external_dag_id=dag.dag_id)

clear_dummy1_downstream >> dummy2_sensor_future >> clear_dummy1_future
