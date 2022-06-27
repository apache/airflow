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

from datetime import datetime

from airflow.models import DAG
from airflow.models.dataset_reference import InletDataset, OutletDataset
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='upstream_dag',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
) as dag1:
    BashOperator(
        outlets=[OutletDataset('s3://abc/dataset3.txt', extra={'hi': 'bye'})],
        inlets=[
            InletDataset('s3://abc/dataset1.txt', schedule_on=False),
            InletDataset('s3://abc/dataset2.txt', schedule_on=False),
        ],
        task_id='upstream_task_1',
        bash_command="sleep 5",
    )

with DAG(
    dag_id='some_downstream_dag',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on_dataset=True,
    schedule_interval=None,
) as dag2:
    BashOperator(
        outlets=[OutletDataset('s3://abc/dataset_other.txt')],
        inlets=[
            InletDataset('s3://abc/dataset3.txt', schedule_on=True),
            InletDataset('s3://abc/dataset_unknown.txt', schedule_on=False),
        ],
        task_id='downstream_1',
        bash_command="sleep 5",
    )

with DAG(
    dag_id='other_downstream_dag',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on_dataset=True,
    schedule_interval=None,
) as dag3:
    BashOperator(
        inlets=[
            InletDataset('s3://abc/dataset3.txt', schedule_on=True),
            InletDataset('s3://abc/dataset_other_unknown.txt', schedule_on=True),
        ],
        task_id='downstream_2',
        bash_command="sleep 5",
    )
