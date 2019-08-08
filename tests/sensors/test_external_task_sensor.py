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
import unittest
from datetime import timedelta, time

from airflow import DAG, settings
from airflow import exceptions
from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import TaskInstance, DagBag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.state import State
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
TEST_TASK_ID = 'time_sensor_check'
DEV_NULL = '/dev/null'


class ExternalTaskSensorTests(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(
            dag_folder=DEV_NULL,
            include_examples=True
        )
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    def test_time_sensor(self):
        t = TimeSensor(
            task_id=TEST_TASK_ID,
            target_time=time(0),
            dag=self.dag
        )
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor(self):
        self.test_time_sensor()
        t = ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            dag=self.dag
        )
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )

    def test_external_dag_sensor(self):

        other_dag = DAG(
            'other_dag',
            default_args=self.args,
            end_date=DEFAULT_DATE,
            schedule_interval='@once')
        other_dag.create_dagrun(
            run_id='test',
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS)
        t = ExternalTaskSensor(
            task_id='test_external_dag_sensor_check',
            external_dag_id='other_dag',
            external_task_id=None,
            dag=self.dag
        )
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )

    def test_templated_sensor(self):
        dag = DAG(TEST_DAG_ID, self.args)

        with dag:
            sensor = ExternalTaskSensor(
                task_id='templated_task',
                external_dag_id='dag_{{ ds }}',
                external_task_id='task_{{ ds }}',
                start_date=DEFAULT_DATE
            )

        instance = TaskInstance(sensor, DEFAULT_DATE)
        instance.render_templates()

        self.assertEqual(sensor.external_dag_id,
                         "dag_{}".format(DEFAULT_DATE.date()))
        self.assertEqual(sensor.external_task_id,
                         "task_{}".format(DEFAULT_DATE.date()))

    def test_external_task_sensor_fn_multiple_execution_dates(self):
        bash_command_code = """
{% set s=execution_date.time().second %}
echo "second is {{ s }}"
if [[ $(( {{ s }} % 60 )) == 1 ]]
    then
        exit 1
fi
exit 0
"""
        dag_external_id = TEST_DAG_ID + '_external'
        dag_external = DAG(
            dag_external_id,
            default_args=self.args,
            schedule_interval=timedelta(seconds=1))
        task_external_with_failure = BashOperator(
            task_id="task_external_with_failure",
            bash_command=bash_command_code,
            retries=0,
            dag=dag_external)
        task_external_without_failure = DummyOperator(
            task_id="task_external_without_failure",
            retries=0,
            dag=dag_external)

        task_external_without_failure.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + timedelta(seconds=1),
            ignore_ti_state=True)

        session = settings.Session()
        TI = TaskInstance
        try:
            task_external_with_failure.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE + timedelta(seconds=1),
                ignore_ti_state=True)
            # The test_with_failure task is excepted to fail
            # once per minute (the run on the first second of
            # each minute).
        except Exception as e:
            failed_tis = session.query(TI).filter(
                TI.dag_id == dag_external_id,
                TI.state == State.FAILED,
                TI.execution_date == DEFAULT_DATE + timedelta(seconds=1)).all()
            if len(failed_tis) == 1 and \
               failed_tis[0].task_id == 'task_external_with_failure':
                pass
            else:
                raise e

        dag_id = TEST_DAG_ID
        dag = DAG(
            dag_id,
            default_args=self.args,
            schedule_interval=timedelta(minutes=1))
        task_without_failure = ExternalTaskSensor(
            task_id='task_without_failure',
            external_dag_id=dag_external_id,
            external_task_id='task_external_without_failure',
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i)
                                          for i in range(2)],
            allowed_states=['success'],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag)
        task_with_failure = ExternalTaskSensor(
            task_id='task_with_failure',
            external_dag_id=dag_external_id,
            external_task_id='task_external_with_failure',
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i)
                                          for i in range(2)],
            allowed_states=['success'],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag)

        task_without_failure.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True)

        with self.assertRaises(AirflowSensorTimeout):
            task_with_failure.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_ti_state=True)

    def test_external_task_sensor_delta(self):
        self.test_time_sensor()
        t = ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_delta=timedelta(0),
            allowed_states=['success'],
            dag=self.dag
        )
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )

    def test_external_task_sensor_fn(self):
        self.test_time_sensor()
        # check that the execution_fn works
        t = ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=lambda dt: dt + timedelta(0),
            allowed_states=['success'],
            dag=self.dag
        )
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )
        # double check that the execution is being called by failing the test
        t2 = ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=lambda dt: dt + timedelta(days=1),
            allowed_states=['success'],
            timeout=1,
            poke_interval=1,
            dag=self.dag
        )
        with self.assertRaises(exceptions.AirflowSensorTimeout):
            t2.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_ti_state=True
            )

    def test_external_task_sensor_error_delta_and_fn(self):
        self.test_time_sensor()
        # Test that providing execution_delta and a function raises an error
        with self.assertRaises(ValueError):
            ExternalTaskSensor(
                task_id='test_external_task_sensor_check_delta',
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                execution_delta=timedelta(0),
                execution_date_fn=lambda dt: dt,
                allowed_states=['success'],
                dag=self.dag
            )

    def test_catch_invalid_allowed_states(self):
        with self.assertRaises(ValueError):
            ExternalTaskSensor(
                task_id='test_external_task_sensor_check',
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                allowed_states=['invalid_state'],
                dag=self.dag
            )

        with self.assertRaises(ValueError):
            ExternalTaskSensor(
                task_id='test_external_task_sensor_check',
                external_dag_id=TEST_DAG_ID,
                external_task_id=None,
                allowed_states=['invalid_state'],
                dag=self.dag
            )

    def test_external_task_sensor_waits_for_task_check_existence(self):
        t = ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id="example_bash_operator",
            external_task_id="non-existing-task",
            check_existence=True,
            dag=self.dag
        )

        with self.assertRaises(AirflowException):
            t.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_ti_state=True
            )

    def test_external_task_sensor_waits_for_dag_check_existence(self):
        t = ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id="non-existing-dag",
            external_task_id=None,
            check_existence=True,
            dag=self.dag
        )

        with self.assertRaises(AirflowException):
            t.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_ti_state=True
            )
