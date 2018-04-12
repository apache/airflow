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

import datetime

import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


class First(PythonOperator):

    def get_the_callable(self):

        def the(**kwargs):
            logging.warning("first thinks the run id is {}".format(kwargs.get('run_id')))

        return the

    def __init__(self):
        super().__init__(
            task_id=self.__class__.__name__,
            python_callable=self.get_the_callable(),
            start_date=datetime.datetime.now(),
            default_args={'provide_context': True}
        )


class Second(PythonOperator):

    def get_the_callable(self):

        def the(**kwargs):
            logging.warning("second thinks the run id is {}".format(kwargs.get('run_id')))

        return the

    def __init__(self):
        super().__init__(
            task_id=self.__class__.__name__,
            python_callable=self.get_the_callable(),
            start_date=datetime.datetime.now(),
            default_args={'provide_context': True}
        )

dag = DAG('the_dagiest_dag_of_all_the_dags')
first = First()
second = Second()
third = BashOperator(task_id='third', bash_command='echo " third {{ run_id }} "', start_date=datetime.datetime.now())
fourth = BashOperator(task_id='fourth', bash_command='echo " fourth {{ run_id }} "', start_date=datetime.datetime.now())
first.dag = dag
second.dag = dag
third.dag = dag
second.set_upstream(first)
third.set_upstream(second)
fourth.set_upstream(third)
