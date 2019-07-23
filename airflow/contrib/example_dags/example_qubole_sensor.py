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
This is only an example DAG to highlight usage of QuboleSensor in various scenarios,
some of these tasks may or may not work based on your QDS account setup.

Run a shell command from Qubole Analyze against your Airflow cluster with following to
trigger it manually `airflow dags trigger example_qubole_sensor`.

*Note: Make sure that connection `qubole_default` is properly set before running
this example.*
"""

from airflow import DAG
from airflow.contrib.sensors.qubole_sensor import QuboleFileSensor, QubolePartitionSensor
from airflow.utils import dates

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('example_qubole_sensor', default_args=default_args, schedule_interval=None)

dag.doc_md = __doc__

t1 = QuboleFileSensor(
    task_id='check_s3_file',
    qubole_conn_id='qubole_default',
    poke_interval=60,
    timeout=600,
    data={
        "files":
            [
                "s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar",
                "s3://paid-qubole/HadoopAPITests/data/{{ ds.split('-')[2] }}.tsv"
            ]  # will check for availability of all the files in array
    },
    dag=dag
)

t2 = QubolePartitionSensor(
    task_id='check_hive_partition',
    poke_interval=10,
    timeout=60,
    data={"schema": "default",
          "table": "my_partitioned_table",
          "columns": [
              {"column": "month", "values":
                  ["{{ ds.split('-')[1] }}"]},
              {"column": "day", "values":
                  ["{{ ds.split('-')[2] }}", "{{ yesterday_ds.split('-')[2] }}"]}
          ]  # will check for partitions like [month=12/day=12,month=12/day=13]
          },
    dag=dag
)

t1.set_downstream(t2)
