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
This is an example DAG which uses the LivyOperator.
The tasks below trigger the computation of pi on the Spark instance
using the Java and Python executables provided in the example library.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator

with DAG(
    dag_id='example_livy_operator',
    default_args={'args': [10]},
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    livy_java_task = LivyOperator(
        task_id='pi_java_task',
        file='/spark-examples.jar',
        num_executors=1,
        conf={
            'spark.shuffle.compress': 'false',
        },
        class_name='org.apache.spark.examples.SparkPi',
    )

    livy_python_task = LivyOperator(task_id='pi_python_task', file='/pi.py', polling_interval=60)

    livy_java_task >> livy_python_task
