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
DAG designed to test a PythonOperator that calls a functool.partial
"""
import functools
import logging
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')


class CallableClass:
    def __call__(self):
        """ A __call__ method """


def a_function(_, __):
    """ A function with two args """


partial_function = functools.partial(a_function, arg_x=1)
class_instance = CallableClass()

logging.info('class_instance type: %s', type(class_instance))

dag = DAG(dag_id='test_task_view_type_check', default_args=default_args)

dag_task1 = PythonOperator(
    task_id='test_dagrun_functool_partial',
    dag=dag,
    python_callable=partial_function,
)

dag_task2 = PythonOperator(
    task_id='test_dagrun_instance',
    dag=dag,
    python_callable=class_instance,
)
